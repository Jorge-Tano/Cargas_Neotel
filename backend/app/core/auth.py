"""
core/auth.py — Autenticación LDAP + JWT
"""
from ldap3 import NONE, Connection, Server, SIMPLE
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from app.core.config import get_settings

settings = get_settings()

ALGORITHM = "HS256"

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

def autenticar_ad(usuario: str, password: str) -> dict | None:
    try:
        print(f"[LDAP] Conectando a {settings.ldap_host}:{settings.ldap_port}")
        print(f"[LDAP] Usuario UPN: {usuario}@{settings.ad_domain}")
        print(f"[LDAP] Base DN: '{settings.ad_base_dn}'")
        server = Server(settings.ldap_host, port=settings.ldap_port, get_info=NONE, connect_timeout=5)
        conn = Connection(
            server,
            user=f"{usuario}@{settings.ad_domain}",
            password=password,
            authentication=SIMPLE,
            auto_bind=True,
            receive_timeout=5,
        )
        print(f"[LDAP] Conexión exitosa, bound: {conn.bound}")
        conn.search(settings.ad_base_dn.strip(), f"(sAMAccountName={usuario})", attributes=["displayName"])
        print(f"[LDAP] Entradas encontradas: {len(conn.entries)}")
        if not conn.entries:
            return None
        nombre = str(conn.entries[0].displayName) if conn.entries[0].displayName else usuario
        conn.unbind()
        return {"usuario": usuario, "nombre": nombre}
    except Exception as e:
        print(f"[LDAP] ERROR {type(e).__name__}: {e}")
        return None

# =============================================================
# JWT
# =============================================================

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def crear_token(data: dict) -> str:
    payload = {**data, "exp": datetime.utcnow() + timedelta(minutes=settings.auth_token_minutes)}
    return jwt.encode(payload, settings.auth_secret_key, algorithm=ALGORITHM)

def verificar_token(token: str = Depends(oauth2_scheme)) -> dict:
    exc = HTTPException(
        status_code=401,
        detail="Sesión inválida o expirada",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.auth_secret_key, algorithms=[ALGORITHM])
        if not payload.get("usuario"):
            raise exc
        return payload
    except JWTError:
        raise exc