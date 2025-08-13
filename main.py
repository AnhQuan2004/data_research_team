# import os, time
# from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Request
# from fastapi.responses import JSONResponse, Response
# from fastapi.middleware.cors import CORSMiddleware
# from google.cloud import storage

# app = FastAPI()

# # Custom CORS middleware
# @app.middleware("http")
# async def cors_handler(request: Request, call_next):
#     # CORS preflight
#     if request.method == "OPTIONS":
#         headers = {
#             "Access-Control-Allow-Origin": "*",
#             "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT, DELETE",
#             "Access-Control-Allow-Headers": "Content-Type",
#             "Access-Control-Max-Age": "3600",
#         }
#         return Response(status_code=204, headers=headers)

#     # Process the request
#     response = await call_next(request)
    
#     # CORS headers for all responses
#     response.headers["Access-Control-Allow-Origin"] = "*"
    
#     return response

# BUCKET = "data_research"
# storage_client = storage.Client()  # trên Cloud Run tự dùng SA đã gán

# MAX_SIZE = 30 * 1024 * 1024  # ~30MB (Cloud Run giới hạn request ~32MiB)

# def build_object_path(proj_id: str, filename: str) -> str:
#     t = time.gmtime()
#     y, m = t.tm_year, f"{t.tm_mon:02d}"
#     # Loại bỏ extension cũ và thêm .csv
#     name_without_ext = filename.rsplit('.', 1)[0] if '.' in filename else filename
#     return f"pending/{y}/{m}/{proj_id.lower()}/{name_without_ext}.csv"

# @app.post("/upload")
# async def upload_csv(
#     file: UploadFile = File(...),
#     proj_id: str = Form(...),
#     filename: str = Form(...),  # Thêm parameter để user nhập tên file
#     uploader: str = Form(default=""),
#     idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
# ):
#     # 1) Kiểm tra loại file
#     if file.content_type not in ("text/csv", "application/vnd.ms-excel"):
#         raise HTTPException(status_code=415, detail="Unsupported Media Type: expected text/csv")

#     # 2) Đọc nội dung (giới hạn kích thước)
#     data = await file.read()
#     if not data:
#         raise HTTPException(status_code=400, detail="Empty file")
#     if len(data) > MAX_SIZE:
#         raise HTTPException(status_code=413, detail="File too large for direct upload; use Signed URL flow")

#     # 3) (Tuỳ chọn) Idempotency: kiểm trùng theo header
#     # Bạn có thể lưu idempotency_key vào Redis/DB để tránh ghi trùng khi client retry.

#     # 4) Ghi lên GCS
#     bucket = storage_client.bucket(BUCKET)
#     object_path = build_object_path(proj_id, filename)  # Truyền filename từ user
#     blob = bucket.blob(object_path)
#     blob.metadata = {"proj_id": proj_id, "uploader": uploader, "schema_version": "v1"}
#     blob.upload_from_string(data, content_type="text/csv")

#     return JSONResponse({
#         "ok": True,
#         "gcs_uri": f"gs://{BUCKET}/{object_path}",
#         "size": len(data),
#         "proj_id": proj_id,
#         "status": "pending"
#     })



import os, time
from datetime import timedelta
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from google.cloud import storage

app = FastAPI()

# ---- CORS đơn giản ----
@app.middleware("http")
async def cors_handler(request: Request, call_next):
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT, DELETE",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, Idempotency-Key",
            "Access-Control-Max-Age": "3600",
        }
        return Response(status_code=204, headers=headers)

    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response

BUCKET = os.getenv("BUCKET_PENDING", "data_research")
MAX_SIZE = 30 * 1024 * 1024  # ~30MB

storage_client = storage.Client()

def build_object_path(proj_id: str, filename: str) -> str:
    t = time.gmtime()
    y, m = t.tm_year, f"{t.tm_mon:02d}"
    name_without_ext = filename.rsplit(".", 1)[0] if "." in filename else filename
    return f"pending/{y}/{m}/{proj_id.lower()}/{name_without_ext}.csv"

# --- Chuẩn hoá UTF-8 + BOM để Excel hiển thị tiếng Việt đúng (tuỳ chọn) ---
FORCE_UTF8_BOM = True
def normalize_csv_bytes(raw: bytes) -> bytes:
    if not FORCE_UTF8_BOM:
        return raw
    try:
        txt = raw.decode("utf-8")
    except UnicodeDecodeError:
        for enc in ("cp1258", "latin-1"):
            try:
                txt = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            txt = raw.decode("utf-8", errors="replace")
    if not txt.startswith("\ufeff"):
        txt = "\ufeff" + txt
    return txt.encode("utf-8")

# ---- Health ----
@app.get("/")
def health():
    return {"ok": True, "service": "csv-uploader", "bucket": BUCKET}

# ---- Upload ----
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
        raise HTTPException(status_code=413, detail="File too large for direct upload; use Signed URL flow")

    data = normalize_csv_bytes(data)

    bucket = storage_client.bucket(BUCKET)
    object_path = build_object_path(proj_id, filename)
    blob = bucket.blob(object_path)
    blob.metadata = {"proj_id": proj_id, "uploader": uploader, "schema_version": "v1"}
    blob.upload_from_string(data, content_type="text/csv; charset=utf-8")

    return JSONResponse({
        "ok": True,
        "gcs_uri": f"gs://{BUCKET}/{object_path}",
        "size": len(data),
        "proj_id": proj_id,
        "status": "pending",
        "object_name": object_path,
    })

# ---- List files ----
@app.get("/files")
def list_files(
    proj_id: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    page_size: int = 50,
    page_token: Optional[str] = None,
):
    if page_size <= 0 or page_size > 1000:
        raise HTTPException(status_code=400, detail="page_size must be 1..1000")

    bucket = storage_client.bucket(BUCKET)
    parts = ["pending"]
    if year:  parts.append(str(year))
    if month: parts.append(f"{int(month):02d}")
    if proj_id: parts.append(proj_id.lower())
    prefix = "/".join(parts)

    it = bucket.list_blobs(prefix=prefix, max_results=page_size, page_token=page_token)
    items = []
    for b in it:
        if b.name.endswith("/"):  # bỏ "thư mục ảo"
            continue
        items.append({
            "name": b.name,
            "gcs_uri": f"gs://{BUCKET}/{b.name}",
            "size": b.size,
            "updated": b.updated.isoformat() if b.updated else None,
            "metadata": b.metadata or {},
        })

    next_token = getattr(it, "next_page_token", None)
    return {"ok": True, "prefix": prefix, "count": len(items), "next_page_token": next_token, "items": items}

# ---- Download (signed URL) ----
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

    if bucket_name != BUCKET:
        raise HTTPException(status_code=403, detail="Access to this bucket is not allowed")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(obj)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="Object not found")

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=int(expires_minutes)),
        method="GET",
        response_disposition=f'attachment; filename="{obj.rsplit("/",1)[-1]}"',
    )
    return {"ok": True, "signed_url": url, "expires_in_minutes": int(expires_minutes)}