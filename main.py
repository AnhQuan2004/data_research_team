import os, time
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage

app = FastAPI()

# Custom CORS middleware
@app.middleware("http")
async def cors_handler(request: Request, call_next):
    # CORS preflight
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT, DELETE",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
        return Response(status_code=204, headers=headers)

    # Process the request
    response = await call_next(request)
    
    # CORS headers for all responses
    response.headers["Access-Control-Allow-Origin"] = "*"
    
    return response

BUCKET = "data_research"
storage_client = storage.Client()  # trên Cloud Run tự dùng SA đã gán

MAX_SIZE = 30 * 1024 * 1024  # ~30MB (Cloud Run giới hạn request ~32MiB)

def build_object_path(proj_id: str, filename: str) -> str:
    t = time.gmtime()
    y, m = t.tm_year, f"{t.tm_mon:02d}"
    # Loại bỏ extension cũ và thêm .csv
    name_without_ext = filename.rsplit('.', 1)[0] if '.' in filename else filename
    return f"pending/{y}/{m}/{proj_id.lower()}/{name_without_ext}.csv"

@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    proj_id: str = Form(...),
    filename: str = Form(...),  # Thêm parameter để user nhập tên file
    uploader: str = Form(default=""),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    # 1) Kiểm tra loại file
    if file.content_type not in ("text/csv", "application/vnd.ms-excel"):
        raise HTTPException(status_code=415, detail="Unsupported Media Type: expected text/csv")

    # 2) Đọc nội dung (giới hạn kích thước)
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(data) > MAX_SIZE:
        raise HTTPException(status_code=413, detail="File too large for direct upload; use Signed URL flow")

    # 3) (Tuỳ chọn) Idempotency: kiểm trùng theo header
    # Bạn có thể lưu idempotency_key vào Redis/DB để tránh ghi trùng khi client retry.

    # 4) Ghi lên GCS
    bucket = storage_client.bucket(BUCKET)
    object_path = build_object_path(proj_id, filename)  # Truyền filename từ user
    blob = bucket.blob(object_path)
    blob.metadata = {"proj_id": proj_id, "uploader": uploader, "schema_version": "v1"}
    blob.upload_from_string(data, content_type="text/csv")

    return JSONResponse({
        "ok": True,
        "gcs_uri": f"gs://{BUCKET}/{object_path}",
        "size": len(data),
        "proj_id": proj_id,
        "status": "pending"
    })