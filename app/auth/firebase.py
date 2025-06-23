import firebase_admin
from firebase_admin import credentials, auth
from fastapi import HTTPException, Request, status
from app.config import settings

# Initialize Firebase Admin SDK only once
if not firebase_admin._apps:
    cred = credentials.Certificate({
        "type": "service_account",
        "project_id": settings.firebase_project_id,
        "private_key": settings.firebase_private_key.replace('\\n', '\n'),
        "client_email": settings.firebase_client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    })
    firebase_admin.initialize_app(cred)

async def get_current_user(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    token = auth_header.split(" ")[1]
    try:
        decoded_token = auth.verify_id_token(token)
        return decoded_token
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")