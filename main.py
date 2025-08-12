import os, time
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import storage

app = FastAPI()
BUCKET = "data_research"
storage_client = storage.Client()  # trên Cloud Run tự dùng SA đã gán

MAX_SIZE = 30 * 1024 * 1024  # ~30MB (Cloud Run giới hạn request ~32MiB)

def build_object_path(proj_id: str) -> str:
    t = time.gmtime()
    y, m = t.tm_year, f"{t.tm_mon:02d}"
    doc_id = str(int(time.time() * 1000))
    return f"pending/{y}/{m}/{proj_id.lower()}/{doc_id}.csv"

@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    proj_id: str = Form(...),
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
    object_path = build_object_path(proj_id)
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
