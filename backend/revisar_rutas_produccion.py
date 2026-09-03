"""Revisa las rutas compartidas configuradas para cada caso — correr en el servidor."""
from app.core.postgres import get_config_global

cfg = get_config_global()
for tipo in ["SAV", "AV", "REFI", "PL", "CARRITO", "MKT"]:
    ruta = cfg.get(f"ruta_{tipo.lower()}_compartida", "")
    print(f"{tipo}: ruta_{tipo.lower()}_compartida = {ruta!r}")
