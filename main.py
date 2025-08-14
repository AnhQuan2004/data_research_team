import time
from typing import Optional
from datetime import timedelta, datetime, timezone

from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Request, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import JSONResponse, Response
from google.cloud import storage
from jose import JWTError, jwt
from pydantic import BaseModel
from typing import Optional
from google.api_core import exceptions as gexc
from pydantic import BaseModel
from pymongo import MongoClient
import bcrypt
import os
from typing import List

app = FastAPI()

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# ===== MongoDB =====
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://admin:mentalhealth@test1.atutk.mongodb.net/?retryWrites=true&w=majority&appName=test1",
)
client = MongoClient(MONGO_URI)
db = client.GFI
users_collection = db.users
audit_log_collection = db.audit_log

# ===== CORS =====
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
    return response

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # For now, we don't need to do anything with incoming messages
    except WebSocketDisconnect:
        manager.disconnect(websocket)

BUCKET = "data_research"
storage_client = storage.Client()
MAX_SIZE = 30 * 1024 * 1024  # 30MB

def build_object_path(status_folder: str, proj_id: str, filename: str) -> str:
    t = time.gmtime()
    y, m = t.tm_year, f"{t.tm_mon:02d}"
    name_without_ext = filename.rsplit(".", 1)[0] if "." in filename else filename
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
        meta = b.metadata or {}
        items.append(
            {
                "name": b.name,
                "gcs_uri": f"gs://{BUCKET}/{b.name}",
                "size": b.size,
                "updated": b.updated.isoformat() if b.updated else None,
                "metadata": meta,
                "feedback": meta.get("feedback", "") if status_folder == "rejected" else None,
            }
        )

    next_token = getattr(it, "next_page_token", None)
    return {"ok": True, "prefix": prefix, "count": len(items), "next_page_token": next_token, "items": items}

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
        expiration=timedelta(minutes=int(expires_minutes)),
        method="GET",
        response_disposition=f'attachment; filename="{obj.rsplit("/",1)[-1]}"',
    )
    return {"ok": True, "signed_url": url}

# ===== Approve =====
@app.post("/approve")
async def approve_file(
    gcs_uri: Optional[str] = None,
    object_name: Optional[str] = None,
    approver: str = "admin",
    current_user: dict = Depends(RoleChecker(["admin"])),
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
    _, year, month, proj_id, filename = parts
    new_path = f"approved/{year}/{month}/{proj_id}/{filename}"

    bucket.copy_blob(blob, bucket, new_path)
    bucket.delete_blob(obj)

    new_blob = bucket.blob(new_path)
    metadata = new_blob.metadata or {}
    metadata.update(
        {
            "status": "approved",
            "approver": approver,
            "approved_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    new_blob.metadata = metadata
    new_blob.patch()

    await manager.broadcast(f"File {obj} approved")
    audit_log_collection.insert_one({
        "action": "approve",
        "object_name": obj,
        "user": current_user["username"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    return {"ok": True, "from": obj, "to": new_path, "status": "approved"}

# ===== Reject =====
@app.post("/reject")
async def reject_object(
    object_name: str,
    rejector: str = "",
    feedback: str = "",
    current_user: dict = Depends(RoleChecker(["admin"])),
):
    try:
        if not object_name.startswith("pending/"):
            raise HTTPException(status_code=400, detail="Only pending/* can be rejected")

        bucket = storage_client.bucket(BUCKET)
        src = bucket.blob(object_name)
        if not src.exists():
            raise HTTPException(status_code=404, detail="Source object not found")

        parts = object_name.split("/", 4)
        dst_name = f"rejected/{parts[1]}/{parts[2]}/{parts[3]}/{parts[4]}"

        bucket.copy_blob(src, bucket, new_name=dst_name)

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

        src.reload()
        bucket.delete_blob(src.name, if_generation_match=src.generation)

        return {
            "ok": True,
            "from": f"gs://{BUCKET}/{object_name}",
            "to": f"gs://{BUCKET}/{dst_name}",
            "status": "rejected",
        }
        await manager.broadcast(f"File {object_name} rejected")
        audit_log_collection.insert_one({
            "action": "reject",
            "object_name": object_name,
            "user": current_user["username"],
            "feedback": feedback,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    except gexc.Forbidden as e:
        raise HTTPException(status_code=403, detail=f"GCS permission error: {e.message}")
    except gexc.NotFound as e:
        raise HTTPException(status_code=404, detail=f"GCS not found: {e.message}")

# ===== Auth (Mongo) =====
SECRET_KEY = "a_very_secret_key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = users_collection.find_one({"username": token_data.username})
    if user is None:
        raise credentials_exception
    return user

class RoleChecker:
    def __init__(self, allowed_roles: list):
        self.allowed_roles = allowed_roles

    def __call__(self, user: dict = Depends(get_current_user)):
        if user["role"] not in self.allowed_roles:
            raise HTTPException(status_code=403, detail="Operation not permitted")

class User(BaseModel):
    username: str
    email: str
    password: str

class LoginUser(BaseModel):
    identifier: str  # username or email
    password: str

@app.post("/register")
async def register_user(user: User):
    if users_collection.find_one({"$or": [{"username": user.username}, {"email": user.email}]}):
        raise HTTPException(status_code=400, detail="Username or email already registered")
    hashed_password = bcrypt.hashpw(user.password.encode("utf-8"), bcrypt.gensalt())
    users_collection.insert_one({"username": user.username, "email": user.email, "password": hashed_password, "role": "user"})
    return {"ok": True, "username": user.username}

@app.post("/login", response_model=Token)
async def login_user(user: LoginUser):
    db_user = users_collection.find_one({"$or": [{"username": user.identifier}, {"email": user.identifier}]})
    if db_user and bcrypt.checkpw(user.password.encode("utf-8"), db_user["password"]):
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": db_user["username"]}, expires_delta=access_token_expires
        )
        return {"access_token": access_token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Invalid credentials")

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt