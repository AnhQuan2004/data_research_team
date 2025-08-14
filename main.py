
import os, time
from typing import Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from google.cloud import storage
from datetime import datetime, timezone
from google.api_core import exceptions as gexc

app = FastAPI()

# ===== CORS middleware =====
@app.middleware("http")
async def cors_handler(request: Request, call_next):
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT, DELETE",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600",
        }
        return Response(status_code=204, headers=headers)
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

BUCKET = "data_research"
storage_client = storage.Client()
MAX_SIZE = 30 * 1024 * 1024  # 30MB

def build_object_path(status_folder: str, proj_id: str, filename: str) -> str:
    t = time.gmtime()
    y, m = t.tm_year, f"{t.tm_mon:02d}"
    name_without_ext = filename.rsplit('.', 1)[0] if '.' in filename else filename
    return f"{status_folder}/{y}/{m}/{proj_id.lower()}/{name_without_ext}.csv"

# ===== Upload =====
@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    proj_id: str = Form(...),
    filename: str = Form(...),
    uploader: str = Form(default=""),
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    if file.content_type not in ("text/csv", "application/vnd.ms-excel"):
        raise HTTPException(status_code=415, detail="Unsupported Media Type: expected text/csv")
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(data) > MAX_SIZE:
        raise HTTPException(status_code=413, detail="File too large")

    bucket = storage_client.bucket(BUCKET)
    object_path = build_object_path("pending", proj_id, filename)
    blob = bucket.blob(object_path)
    blob.metadata = {
        "proj_id": proj_id,
        "uploader": uploader,
        "schema_version": "v1",
        "status": "pending",
    }
    blob.upload_from_string(data, content_type="text/csv")
    return {"ok": True, "gcs_uri": f"gs://{BUCKET}/{object_path}", "status": "pending"}

# ===== List =====
@app.get("/files")
def list_files(
    status_folder: str = "pending",
    proj_id: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    page_size: int = 50,
    page_token: Optional[str] = None,
):
    bucket = storage_client.bucket(BUCKET)
    parts = [status_folder]
    if year:
        parts.append(str(year))
    if month:
        parts.append(f"{int(month):02d}")
    if proj_id:
        parts.append(proj_id.lower())
    prefix = "/".join(parts)
    it = bucket.list_blobs(prefix=prefix, max_results=page_size, page_token=page_token)
    items = []
    for b in it:
        if b.name.endswith("/"):
            continue
        items.append({
            "name": b.name,
            "gcs_uri": f"gs://{BUCKET}/{b.name}",
            "size": b.size,
            "updated": b.updated.isoformat() if b.updated else None,
            "metadata": b.metadata or {},
            "feedback": b.metadata.get("feedback", "") if status_folder == "rejected" else None
        })
    return {"ok": True, "prefix": prefix, "count": len(items), "items": items}

# ===== Download (Signed URL) =====
@app.get("/download")
def get_signed_download_url(
    gcs_uri: Optional[str] = None,
    object_name: Optional[str] = None,
    expires_minutes: int = 15,
):
    if not gcs_uri and not object_name:
        raise HTTPException(status_code=400, detail="Provide gcs_uri or object_name")
    if gcs_uri:
        if not gcs_uri.startswith("gs://"):
            raise HTTPException(status_code=400, detail="gcs_uri must start with gs://")
        without = gcs_uri[5:]
        bucket_name, obj = without.split("/", 1)
    else:
        bucket_name, obj = BUCKET, object_name
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(obj)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="Object not found")
    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=expires_minutes),
        method="GET",
        response_disposition=f'attachment; filename="{obj.rsplit("/",1)[-1]}"',
    )
    return {"ok": True, "signed_url": url}

# ===== Approve =====
@app.post("/approve")
def approve_file(
    gcs_uri: Optional[str] = None,
    object_name: Optional[str] = None,
    approver: str = "admin"
):
    if not gcs_uri and not object_name:
        raise HTTPException(status_code=400, detail="Provide gcs_uri or object_name")
    if gcs_uri:
        without = gcs_uri[5:]
        bucket_name, obj = without.split("/", 1)
    else:
        bucket_name, obj = BUCKET, object_name

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(obj)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="Object not found")

    # Extract filename & proj_id
    parts = obj.split("/")
    if parts[0] != "pending":
        raise HTTPException(status_code=400, detail="Only pending files can be approved")
    _, year, month, proj_id, filename = parts
    new_path = f"approved/{year}/{month}/{proj_id}/{filename}"

    # Copy -> Delete
    bucket.copy_blob(blob, bucket, new_path)
    bucket.delete_blob(obj)

    # Update metadata for new file
    new_blob = bucket.blob(new_path)
    metadata = new_blob.metadata or {}
    metadata.update({"status": "approved", "approver": approver, "approved_at": datetime.utcnow().isoformat()})
    new_blob.metadata = metadata
    new_blob.patch()

    return {"ok": True, "from": obj, "to": new_path, "status": "approved"}


# ===== Reject =====
@app.post("/reject")
def reject_object(
    object_name: str,           # ví dụ: pending/2025/08/solana/xxx.csv
    rejector: str = "",         # người reject
    feedback: str = "",         # feedback chi tiết
):
    try:
        if not object_name.startswith("pending/"):
            raise HTTPException(status_code=400, detail="Only pending/* can be rejected")

        bucket = storage_client.bucket(BUCKET)
        src = bucket.blob(object_name)
        if not src.exists():
            raise HTTPException(status_code=404, detail="Source object not found")

        # pending/YYYY/MM/proj/file.csv -> rejected/YYYY/MM/proj/file.csv
        parts = object_name.split("/", 4)
        dst_name = f"rejected/{parts[1]}/{parts[2]}/{parts[3]}/{parts[4]}"

        # copy sang rejected/...
        bucket.copy_blob(src, bucket, new_name=dst_name)

        # cập nhật metadata ở file đích
        dst = bucket.blob(dst_name)
        dst.reload()
        md = dst.metadata or {}
        md.update({
            "status": "rejected",
            "rejected_by": rejector or "",
            "rejected_at": datetime.now(timezone.utc).isoformat(),
            "feedback": feedback or "",            # << lưu feedback
        })
        dst.metadata = md
        dst.patch()

        # xoá nguồn, tránh race condition
        src.reload()
        bucket.delete_blob(src.name, if_generation_match=src.generation)

        return {
            "ok": True,
            "from": f"gs://{BUCKET}/{object_name}",
            "to": f"gs://{BUCKET}/{dst_name}",
            "status": "rejected"
        }

    except gexc.Forbidden as e:
        raise HTTPException(status_code=403, detail=f"GCS permission error: {e.message}")
    except gexc.NotFound as e:
        raise HTTPException(status_code=404, detail=f"GCS not found: {e.message}")
