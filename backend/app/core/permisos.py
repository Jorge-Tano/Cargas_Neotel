"""
Permisos por usuario — qué secciones/casos ve cada quien al iniciar sesión.
─────────────────────────────────────────────
Reemplaza los chequeos que antes estaban fijos en el código (esAdmin para
Carga Mensual/Bases, "jorge.gomez" a mano para Amalia/Op. Pago/Bases):
ahora es 100% configurable desde la UI (Configuración → Permisos, solo
admin) y se guarda en Postgres (tabla `permisos_usuario`).

Los administradores (rol == "admin", ver auth.es_admin) siempre ven todo
— la tabla de permisos solo aplica a usuarios no-admin. Un usuario sin
ninguna fila guardada usa PERMISOS_DEFAULT, que replica el comportamiento
que ya existía antes de este sistema (para no cambiarle nada a nadie
hasta que un admin decida personalizarlo).
"""

# Secciones del menú lateral (ver frontend NAV_ITEMS)
ITEMS_NAV = {
    "procesar":      True,
    "carga-mensual": False,
    "bases":         False,
    "historial":     True,
    "lista-negra":   True,
    "repetidos":     True,
    "configuracion": True,
    "watcher":       True,
}

# Casos dentro del panel "Procesar" (ver frontend CASOS en lib/api.ts)
ITEMS_CASOS = {
    "SAV":         True,
    "AV":          True,
    "REFI":        True,
    "PL":          True,
    "MKT":         True,
    "CARRITO":     True,
    "PERDIDAS":    True,
    "AMALIA":      False,
    "OP_PERDIDAS": False,
    "OP_WHATSAPP": False,
}

# Por cada caso, además de "puede ver/usar" (ITEMS_CASOS) hay un segundo
# permiso: "puede procesar en Neotel" (vs. quedar forzado a "solo proceso
# interno" sin importar lo que elija en el toggle de la tarjeta). Antes
# esto vivía fijo en PERDIDAS_USUARIO_AUTORIZADO (.env, un solo usuario,
# solo para PERDIDAS/AMALIA/OP_*) — ahora es 100% por usuario y aplica a
# los 10 casos. Default: igual al comportamiento previo (SAV/AV/REFI/PL/
# MKT/CARRITO sin restricción; PERDIDAS/AMALIA/OP_* solo para admin, que
# ya ve todo en True siempre).
ITEMS_CASOS_NEOTEL = {f"{caso}_neotel": (caso not in ("PERDIDAS", "AMALIA", "OP_PERDIDAS", "OP_WHATSAPP")) for caso in ITEMS_CASOS}

# Los otros ítems del menú que también tienen sub-opciones propias (PL/REFI
# en cada caso) — mismo patrón que ITEMS_CASOS pero sin el segundo permiso
# "_neotel" (Carga Mensual y Bases no tienen un modo "solo interno").
ITEMS_CARGA_MENSUAL = {
    "carga_mensual_pl":   True,
    "carga_mensual_refi": True,
}
ITEMS_BASES = {
    "bases_pl":   True,
    "bases_refi": True,
}

PERMISOS_DEFAULT: dict[str, bool] = {
    **ITEMS_NAV, **ITEMS_CASOS, **ITEMS_CASOS_NEOTEL,
    **ITEMS_CARGA_MENSUAL, **ITEMS_BASES,
}


def permisos_efectivos(usuario: str, rol: str) -> dict[str, bool]:
    """Permisos reales a aplicar para este usuario: todo permitido si es
    admin, si no el default con lo que haya guardado en Postgres encima."""
    if rol == "admin":
        return {k: True for k in PERMISOS_DEFAULT}

    from app.core.postgres import get_permisos_usuario
    guardados = get_permisos_usuario(usuario)
    return {**PERMISOS_DEFAULT, **guardados}
