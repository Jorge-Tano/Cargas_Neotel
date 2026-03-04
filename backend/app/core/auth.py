"""
core/auth.py — Lógica de autenticación: LDAP + JWT
"""
from ldap3 import NONE, Connection, Server, SIMPLE
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

# =============================================================
# CONFIG — leído desde .env
# =============================================================
SECRET_KEY    = os.getenv("AUTH_SECRET_KEY")
ALGORITHM     = "HS256"
TOKEN_MINUTES = int(os.getenv("AUTH_TOKEN_MINUTES"))

LDAP_HOST  = os.getenv("LDAP_HOST")
LDAP_PORT  = int(os.getenv("LDAP_PORT"))
AD_DOMAIN  = os.getenv("AD_DOMAIN",  )
AD_BASE_DN = os.getenv("AD_BASE_DN")

# =============================================================
# MODELOS
# =============================================================
class LoginRequest(BaseModel):
    usuario: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    nombre: str

# =============================================================
# LDAP
# =============================================================
def autenticar_ad(usuario: str, password: str):
    try:
        print(f"[LDAP] Conectando a {LDAP_HOST}:{LDAP_PORT}")
        print(f"[LDAP] Usuario UPN: {usuario}@{AD_DOMAIN}")
        print(f"[LDAP] Base DN: '{AD_BASE_DN}'")
        server = Server(LDAP_HOST, port=LDAP_PORT, get_info=NONE, connect_timeout=5)
        conn = Connection(
            server,
            user=f"{usuario}@{AD_DOMAIN}",
            password=password,
            authentication=SIMPLE,
            auto_bind=True,
            receive_timeout=5,
        )
        print(f"[LDAP] Conexion exitosa, bound: {conn.bound}")
        conn.search(AD_BASE_DN.strip(), f"(sAMAccountName={usuario})", attributes=["displayName", "mail"])
        print(f"[LDAP] Entradas encontradas: {len(conn.entries)}")
        print(f"[LDAP] Resultado: {conn.result}")
        if not conn.entries:
            return None
        entry = conn.entries[0]
        nombre = str(entry.displayName) if entry.displayName else usuario
        conn.unbind()
        return {"usuario": usuario, "nombre": nombre}
    except Exception as e:
        print(f"[LDAP] ERROR tipo: {type(e).__name__}")
        print(f"[LDAP] ERROR detalle: {e}")
        return None
# =============================================================
# JWT
# =============================================================
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def crear_token(data: dict) -> str:
    payload = {**data, "exp": datetime.utcnow() + timedelta(minutes=TOKEN_MINUTES)}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verificar_token(token: str = Depends(oauth2_scheme)) -> dict:
    exc = HTTPException(
        status_code=401,
        detail="Sesion invalida o expirada",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if not payload.get("usuario"):
            raise exc
        return payload
    except JWTError:
        raise exc