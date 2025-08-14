# import os, time
# from typing import Optional
# from datetime import timedelta

# from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Request
# from fastapi.responses import JSONResponse, Response
# from fastapi.middleware.cors import CORSMiddleware
# from google.cloud import storage

# app = FastAPI()

# # Custom CORS middleware (unchanged)
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

# # =========================
# # Upload (kept exactly the same as you sent)
# # =========================
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
#     blob.metadata = {"proj_id": proj_id, "uploader": uploader, "schema_version": "v1", "status": "pending"}
#     blob.upload_from_string(data, content_type="text/csv")

#     return JSONResponse({
#         "ok": True,
#         "gcs_uri": f"gs://{BUCKET}/{object_path}",
#         "size": len(data),
#         "proj_id": proj_id,
#         "status": "pending"
#     })

# # =========================
# # List files with filters + pagination
# # =========================
# @app.get("/files")
# def list_files(
#     proj_id: Optional[str] = None,
#     year: Optional[int] = None,
#     month: Optional[int] = None,
#     page_size: int = 50,
#     page_token: Optional[str] = None,
# ):
#     if page_size <= 0 or page_size > 1000:
#         raise HTTPException(status_code=400, detail="page_size must be 1..1000")

#     bucket = storage_client.bucket(BUCKET)

#     # prefix: pending/<year>/<month>/<proj_id> (có thể thiếu các phần nếu không filter)
#     parts = ["pending"]
#     if year:
#         parts.append(str(year))
#     if month:
#         parts.append(f"{int(month):02d}")
#     if proj_id:
#         parts.append(proj_id.lower())
#     prefix = "/".join(parts)

#     it = bucket.list_blobs(prefix=prefix, max_results=page_size, page_token=page_token)

#     items = []
#     for b in it:
#         if b.name.endswith("/"):  # bỏ thư mục ảo
#             continue
#         items.append({
#             "name": b.name,
#             "gcs_uri": f"gs://{BUCKET}/{b.name}",
#             "size": b.size,
#             "updated": b.updated.isoformat() if b.updated else None,
#             "metadata": b.metadata or {},
#             "status": b.metadata.get("status", "pending")
#         })

#     next_token = getattr(it, "next_page_token", None)

#     return {
#         "ok": True,
#         "prefix": prefix,
#         "count": len(items),
#         "next_page_token": next_token,
#         "items": items
#     }

# # =========================
# # Download (Signed URL)
# # =========================
# @app.get("/download")
# def get_signed_download_url(
#     gcs_uri: Optional[str] = None,
#     object_name: Optional[str] = None,
#     expires_minutes: int = 15,
# ):
#     """
#     Trả về signed URL để tải file trực tiếp từ GCS.
#     Truyền EITHER:
#       - gcs_uri="gs://data_research/pending/2025/08/solana/xyz.csv"
#       - object_name="pending/2025/08/solana/xyz.csv"
#     """
#     if not gcs_uri and not object_name:
#         raise HTTPException(status_code=400, detail="Provide gcs_uri or object_name")

#     # Parse gs:// nếu cần
#     if gcs_uri:
#         if not gcs_uri.startswith("gs://"):
#             raise HTTPException(status_code=400, detail="gcs_uri must start with gs://")
#         without = gcs_uri[5:]
#         bucket_name, obj = without.split("/", 1)
#     else:
#         bucket_name, obj = BUCKET, object_name

#     # Chỉ cho phép tải từ đúng bucket cấu hình
#     if bucket_name != BUCKET:
#         raise HTTPException(status_code=403, detail="Access to this bucket is not allowed")

#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(obj)
#     if not blob.exists():
#         raise HTTPException(status_code=404, detail="Object not found")

#     url = blob.generate_signed_url(
#         version="v4",
#         expiration=timedelta(minutes=int(expires_minutes)),
#         method="GET",
#         response_disposition=f'attachment; filename="{obj.rsplit("/",1)[-1]}"',
#     )
#     return {"ok": True, "signed_url": url, "expires_in_minutes": int(expires_minutes)}

import os, time
from typing import Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from google.cloud import storage

from pydantic import BaseModel
from pymongo import MongoClient
import bcrypt
import os

app = FastAPI()

# MongoDB setup
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://admin:mentalhealth@test1.atutk.mongodb.net/?retryWrites=true&w=majority&appName=test1")
client = MongoClient(MONGO_URI)
db = client.GFI
users_collection = db.users

# ===== CORS middleware =====
@app.middleware("http")
async def cors_handler(request: Request, call_next):
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS, PUT, DELETE",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
        return Response(status_code=204, headers=headers)
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
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



# ===== User Authentication =====
class User(BaseModel):
    username: str
    email: str
    password: str

class LoginUser(BaseModel):
    identifier: str # username or email
    password: str

@app.post("/register")
async def register_user(user: User):
    # Check if username or email already exists
    if users_collection.find_one({"$or": [{"username": user.username}, {"email": user.email}]}):
        raise HTTPException(status_code=400, detail="Username or email already registered")
    
    # Hash the password
    hashed_password = bcrypt.hashpw(user.password.encode('utf-8'), bcrypt.gensalt())
    
    # Insert the new user
    users_collection.insert_one({
        "username": user.username,
        "email": user.email,
        "password": hashed_password
    })
    
    return {"ok": True, "username": user.username}

@app.post("/login")
async def login_user(user: LoginUser):
    # Find the user by username or email
    db_user = users_collection.find_one({"$or": [{"username": user.identifier}, {"email": user.identifier}]})
    
    # Check if the user exists and the password is correct
    if db_user and bcrypt.checkpw(user.password.encode('utf-8'), db_user["password"]):
        return {"ok": True, "username": db_user["username"]}
    
    raise HTTPException(status_code=401, detail="Invalid credentials")