import os
import jwt
from datetime import datetime, timedelta
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv

load_dotenv(dotenv_path="../dados_rfb_env/.env")

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
JWT_EXPIRATION_HOURS = int(os.getenv("JWT_EXPIRATION_HOURS", 24))

SNFISC_CLIENT_ID = "snfisc"
SNFISC_CLIENT_SECRET = os.getenv("SNFISC_CLIENT_SECRET", "snfisc-secret-change-in-production")

security = HTTPBearer()

def create_token(client_id: str, client_secret: str) -> str:
    """Cria JWT token após validar credentials"""
    if client_id == SNFISC_CLIENT_ID and client_secret == SNFISC_CLIENT_SECRET:
        payload = {
            "sub": client_id,
            "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS),
            "iat": datetime.utcnow(),
        }
        token = jwt.encode(payload, JWT_SECRET_KEY, algorithm="HS256")
        return token
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """Valida JWT token do header Authorization: Bearer {token}"""
    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
        client_id: str = payload.get("sub")
        if client_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return client_id
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
