"""
Diagnostico LDAP aislado: hace el mismo bind que autenticar_ad(), pero
sin pasar por FastAPI/JSON/navegador. Sirve para descartar que el
problema este en la app y no en la cuenta/AD.

Uso:
    ./.venv/Scripts/python.exe debug_ldap.py <usuario> <password>
"""
import sys
from ldap3 import NONE, Connection, Server, SIMPLE
from app.core.config import get_settings

settings = get_settings()

if len(sys.argv) != 3:
    print("Uso: python debug_ldap.py <usuario> <password>")
    sys.exit(1)

usuario, password = sys.argv[1], sys.argv[2]
upn = f"{usuario}@{settings.ad_domain}"

print(f"Host: {settings.ldap_host}:{settings.ldap_port}")
print(f"UPN:  {upn}")
print(f"Base DN: {settings.ad_base_dn}")
print(f"Password length: {len(password)} caracteres")
print()

server = Server(settings.ldap_host, port=settings.ldap_port, get_info=NONE, connect_timeout=5)

try:
    conn = Connection(
        server,
        user=upn,
        password=password,
        authentication=SIMPLE,
        auto_bind=True,
        receive_timeout=5,
    )
    print("✅ BIND EXITOSO")
    print(f"bound: {conn.bound}")

    conn.search(settings.ad_base_dn.strip(), f"(sAMAccountName={usuario})", attributes=["displayName", "userAccountControl", "userPrincipalName", "distinguishedName"])
    print(f"\nEntradas encontradas dentro de {settings.ad_base_dn}: {len(conn.entries)}")
    for e in conn.entries:
        print(e)
    conn.unbind()

except Exception as e:
    print(f"❌ BIND FALLÓ: {type(e).__name__}: {e}")

    # Reintento con el server admin/lectura (si esta configurado) para
    # inspeccionar la cuenta SIN autenticarse como ella, y ver su estado real
    # (bloqueada, deshabilitada, UPN real) — solo si hay credenciales de
    # servicio separadas; si no, este bloque se omite.
