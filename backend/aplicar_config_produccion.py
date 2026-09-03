"""
aplicar_config_produccion.py
=============================
Aplica en Postgres todo lo que se armó en la sesión de hoy (tabla de
confirmacion de carga, IDs de MKT/Carrito, fix de guardar_compartida)
usando las MISMAS funciones que ya usa el backend — así se conecta a
la base que indique el .env DE ESTE SERVIDOR, no a ninguna otra.

Uso (correr UNA vez, en el servidor, con el venv del backend activo):
    python aplicar_config_produccion.py

Es seguro correrlo mas de una vez (las partes que ya existan/coincidan
no se tocan).
"""

from app.core.postgres import init_tables, get_config_global, set_config_global, get_config_usuario, set_config_usuario

print("=== 1. Creando/actualizando tablas (log_confirmacion_carga, etc.) ===")
init_tables()

print("\n=== 2. IDs de MKT y Carrito (misma BD de Neotel real, no cambia entre entornos) ===")
set_config_global({
    "DB_MKT":            "ECRM_0035",
    "IDDATABASE_MKT":    "17",
    "DB_CARRITO":        "ECRM_0035",
    "IDDATABASE_CARRITO": "13",
})
cfg = get_config_global()
for k in ["DB_MKT", "IDDATABASE_MKT", "DB_CARRITO", "IDDATABASE_CARRITO"]:
    print(f"  {k} = {cfg.get(k)}")

print("\n=== 3. Fix urgente: guardar_compartida para el usuario del watcher ===")
usuario = cfg.get("usuario_watcher_rutas", "").strip() or "jorge.gomez"
print(f"  usuario_watcher_rutas = {usuario!r}")
u_cfg = get_config_usuario(usuario)
for tipo in ["SAV", "AV", "REFI", "PL", "CARRITO", "MKT"]:
    actual = u_cfg.get(tipo, {})
    antes = actual.get("guardar_compartida")
    set_config_usuario(
        usuario=usuario,
        tipo=tipo,
        ruta_local=actual.get("ruta_local", ""),
        guardar_local=actual.get("guardar_local", False),
        guardar_compartida=True,
    )
    print(f"  {tipo}: guardar_compartida {antes} -> True")

print("\n=== Listo. Revisa que las rutas ruta_{tipo}_compartida existan y sean accesibles desde este servidor. ===")
