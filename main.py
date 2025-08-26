import os
import time
import uuid
import json
from typing import Optional
from datetime import datetime, timedelta, timezone

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    Form,
    Header,
    HTTPException,
    Request,
    Body,
    Query,
)
from fastapi.responses import Response
from google.cloud import storage
from google.api_core import exceptions as gexc

# =========================
# Config
# =========================
app = FastAPI()

BUCKET = os.getenv("BUCKET", "data_research")
MAX_SIZE_MB = int(os.getenv("MAX_SIZE_MB", "30"))
MAX_SIZE = MAX_SIZE_MB * 1024 * 1024  # default 30MB

storage_client = storage.Client()


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

# =========================
# Helpers
# =========================
def require_bucket() -> storage.Bucket:
    if not BUCKET:
        raise HTTPException(status_code=500, detail="BUCKET env is not set")
    return storage_client.bucket(BUCKET)


def build_object_path_csv(status_folder: str, proj_id: str, filename: str) -> str:
    """Always writes .csv"""
    t = time.gmtime()
    y, m = t.tm_year, f"{t.tm_mon:02d}"
    name_without_ext = filename.rsplit(".", 1)[0] if "." in filename else filename
    return f"{status_folder}/{y}/{m}/{proj_id.lower()}/{name_without_ext}.csv"


# =========================
# Health
# =========================
@app.get("/healthz")
def healthz():
    return {"ok": True, "bucket": BUCKET, "max_size_mb": MAX_SIZE_MB}


# =========================
# Upload CSV
# =========================
@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    proj_id: str = Form(...),
    filename: str = Form(...),
    uploader: str = Form(default=""),
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    if file.content_type not in ("text/csv", "application/vnd.ms-excel", "application/csv"):
        raise HTTPException(status_code=415, detail="Unsupported Media Type: expected text/csv")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(data) > MAX_SIZE:
        raise HTTPException(status_code=413, detail=f"File too large (> {MAX_SIZE_MB}MB)")

    bucket = require_bucket()
    object_path = build_object_path_csv("pending", proj_id, filename)
    blob = bucket.blob(object_path)
    blob.metadata = {
        "proj_id": proj_id,
        "uploader": uploader,
        "schema_version": "v1",
        "status": "pending",
        "content": "csv",
        "idempotency_key": idempotency_key or "",
    }

    blob.upload_from_string(data, content_type="text/csv")
    return {"ok": True, "gcs_uri": f"gs://{BUCKET}/{object_path}", "status": "pending"}


# =========================
# Submit JSON (NO filename; split by project_id)
# =========================
@app.post("/submit-json")
async def submit_json_v3(
    payload: dict = Body(..., media_type="application/json"),
    uploader: Optional[str] = Query(default=""),
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    """
    Payload:
    {
      "questions_main": [
        { "project_id": "...", "q_id": ..., "question": "...", "result": 0|1, "detail": "...", "source": "..." },
        ...
      ]
    }
    - Group theo project_id
    - Tự sinh tên file: pending/YYYY/MM/<project_id>/<YYYYMMDDTHHMMSS>-<uuid6>.json
    - Mỗi project_id -> 1 file chứa chỉ các item của project đó
    """
    # --- Validate tổng thể ---
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object.")
    if "questions_main" not in payload or not isinstance(payload["questions_main"], list):
        raise HTTPException(status_code=400, detail="questions_main must be a list.")
    if not payload["questions_main"]:
        raise HTTPException(status_code=400, detail="questions_main cannot be empty.")

    # --- Validate từng item & group theo project_id ---
    groups = {}
    for idx, item in enumerate(payload["questions_main"], start=1):
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail=f"Item #{idx} must be an object.")
        required = ["project_id", "q_id", "question", "result", "detail", "source"]
        miss = [k for k in required if k not in item]
        if miss:
            raise HTTPException(status_code=400, detail=f"Item #{idx} missing fields: {', '.join(miss)}")

        project_id = item["project_id"]
        if not isinstance(project_id, str) or not project_id.strip():
            raise HTTPException(status_code=400, detail=f"Item #{idx}: project_id must be string.")
        project_id = project_id.lower().strip()

        # normalize result -> int in {0,1}
        if item["result"] not in (0, 1, "0", "1"):
            raise HTTPException(status_code=400, detail=f"Item #{idx}: result must be 0 or 1.")
        item["result"] = int(item["result"])

        # basic checks for strings
        for k in ["question", "detail", "source"]:
            if not isinstance(item[k], str) or not item[k].strip():
                raise HTTPException(status_code=400, detail=f"Item #{idx}: {k} must be a non-empty string.")
            item[k] = item[k].strip()

        groups.setdefault(project_id, []).append(item)

    # --- Chuẩn bị & ghi GCS ---
    bucket = require_bucket()
    results = []
    now = datetime.utcnow()
    ts = now.strftime("%Y%m%dT%H%M%S")
    year = now.year
    month = f"{now.month:02d}"

    for proj, items in groups.items():
        file_payload = {"questions_main": items}
        data_bytes = json.dumps(file_payload, ensure_ascii=False).encode("utf-8")
        if len(data_bytes) > MAX_SIZE:
            raise HTTPException(status_code=413, detail=f"Payload for {proj} too large (> {MAX_SIZE_MB}MB)")

        suffix = uuid.uuid4().hex[:6]
        object_path = f"pending/{year}/{month}/{proj}/{ts}-{suffix}.json"

        blob = bucket.blob(object_path)
        md = {
            "proj_id": proj,
            "uploader": uploader or "",
            "schema_version": "v3",
            "status": "pending",
            "content": "json",
            "idempotency_key": idempotency_key or "",
            "items_count": str(len(items)),
        }
        blob.metadata = md
        blob.upload_from_string(data_bytes, content_type="application/json")

        results.append(
            {
                "project_id": proj,
                "gcs_uri": f"gs://{BUCKET}/{object_path}",
                "count": len(items),
                "status": "pending",
            }
        )

    return {"ok": True, "written": results}


# =========================
# List files
# =========================
@app.get("/files")
def list_files(
    status_folder: str = Query(default="pending"),
    proj_id: Optional[str] = Query(default=None),
    year: Optional[int] = Query(default=None),
    month: Optional[int] = Query(default=None),
    page_size: int = Query(default=50, ge=1, le=1000),
    page_token: Optional[str] = Query(default=None),
):
    bucket = require_bucket()
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
        # Skip "directory" placeholders
        if b.name.endswith("/"):
            continue
        md = b.metadata or {}
        items.append(
            {
                "name": b.name,
                "gcs_uri": f"gs://{BUCKET}/{b.name}",
                "size": b.size,
                "updated": b.updated.isoformat() if b.updated else None,
                "metadata": md,
                "feedback": md.get("feedback", "") if status_folder == "rejected" else None,
            }
        )

    next_token = getattr(it, "next_page_token", None)
    return {"ok": True, "prefix": prefix, "count": len(items), "items": items, "next_page_token": next_token}


# =========================
# Download (Signed URL)
# =========================
@app.get("/download")
def get_signed_download_url(
    gcs_uri: Optional[str] = Query(default=None),
    object_name: Optional[str] = Query(default=None),
    expires_minutes: int = Query(default=15, ge=1, le=1440),
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


# =========================
# Approve (pending -> approved)
# =========================
@app.post("/approve")
def approve_file(
    gcs_uri: Optional[str] = Query(default=None),
    object_name: Optional[str] = Query(default=None),
    approver: str = Query(default="admin"),
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

    parts = obj.split("/")
    if parts[0] != "pending":
        raise HTTPException(status_code=400, detail="Only pending files can be approved")
    if len(parts) < 5:
        raise HTTPException(status_code=400, detail="Invalid object path format")
    _, year, month, proj_id, filename = parts
    new_path = f"approved/{year}/{month}/{proj_id}/{filename}"

    # Copy -> Delete
    bucket.copy_blob(blob, bucket, new_path)
    bucket.delete_blob(obj)

    # Update metadata for new file
    new_blob = bucket.blob(new_path)
    md = new_blob.metadata or {}
    md.update(
        {
            "status": "approved",
            "approver": approver,
            "approved_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    new_blob.metadata = md
    new_blob.patch()

    return {"ok": True, "from": obj, "to": new_path, "status": "approved"}


# =========================
# Reject (pending -> rejected) + feedback
# =========================
@app.post("/reject")
def reject_object(
    object_name: str = Query(..., description="e.g. pending/2025/08/solana/xxx.json"),
    rejector: str = Query(default=""),
    feedback: str = Query(default=""),
):
    try:
        if not object_name.startswith("pending/"):
            raise HTTPException(status_code=400, detail="Only pending/* can be rejected")

        bucket = require_bucket()
        src = bucket.blob(object_name)
        if not src.exists():
            raise HTTPException(status_code=404, detail="Source object not found")

        # pending/YYYY/MM/proj/file.ext -> rejected/YYYY/MM/proj/file.ext
        parts = object_name.split("/", 4)
        if len(parts) < 5:
            raise HTTPException(status_code=400, detail="Invalid object path format")
        dst_name = f"rejected/{parts[1]}/{parts[2]}/{parts[3]}/{parts[4]}"

        # copy sang rejected/...
        bucket.copy_blob(src, bucket, new_name=dst_name)

        # cập nhật metadata ở file đích
        dst = bucket.blob(dst_name)
        dst.reload()
        md = dst.metadata or {}
        md.update(
            {
                "status": "rejected",
                "rejected_by": rejector or "",
                "rejected_at": datetime.now(timezone.utc).isoformat(),
                "feedback": feedback or "",
            }
        )
        dst.metadata = md
        dst.patch()

        # xoá nguồn, tránh race condition
        src.reload()
        bucket.delete_blob(src.name, if_generation_match=src.generation)

        return {
            "ok": True,
            "from": f"gs://{BUCKET}/{object_name}",
            "to": f"gs://{BUCKET}/{dst_name}",
            "status": "rejected",
        }

    except gexc.Forbidden as e:
        raise HTTPException(status_code=403, detail=f"GCS permission error: {e.message}")
    except gexc.NotFound as e:
        raise HTTPException(status_code=404, detail=f"GCS not found: {e.message}")