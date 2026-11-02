"""
regla_discado.py
=================
Cambia sola, cada día, la "Regla discado" de la base Leakage en Neotel.

Cada registro de SAV se carga con "Orden Discado" = día del mes de su
carga (ver sav_av._construir_carga_sav), y la base tiene una regla que deja
discar solo los del día:
    (ISNULL(intOrdenDiscado,0) = 27)
Antes había que cambiar ese número a mano todas las mañanas (pantalla de
la base en Neotel → "Regla discado"). Ahora, apenas se confirma en Neotel
el primer lote del día de un caso habilitado, se revisa la regla de la
base donde entró ese lote y, si no es la del día, se cambia con la misma
función que usa la pantalla Bases de la app
(campanas_mensuales.actualizar_regla → Tarea 83, TipoOperacion "REGLA"),
que vuelve a leerla para confirmar.

Es idempotente: con la regla ya en el día de hoy no hace nada, así que se
puede llamar después de cada lote sin costo (una consulta).

Además, con cada lote se rearma la fila del discador para que se llame
primero lo último cargado (ver _reordenar_fila).

Casos habilitados en config_global REGLA_DISCADO_CASOS (separados por
coma, ej. "SAV_AV" o "SAV_AV,REFI"); sin configurar, solo SAV_AV; con
"NINGUNO", apagado.
"""

from __future__ import annotations

import logging
import threading
import time

from app.core.config import get_settings
from app.core.postgres import get_config_valor

logger = logging.getLogger(__name__)
settings = get_settings()

# Caso de la carga diaria → tipo de la pantalla Bases (campanas_mensuales).
_TIPO_BASES = {"SAV_AV": "SAV", "REFI": "REFI", "PL": "PL"}
_CASOS_DEFAULT = "SAV_AV"
_CATEGORIA = "bases_regla"  # Configuración → Notificaciones (app.core.notificaciones)
_lock = threading.Lock()


def casos_habilitados() -> set[str]:
    """Sin configurar (get_config_valor devuelve "" si la clave no existe)
    → solo SAV_AV. Para apagarlo del todo: REGLA_DISCADO_CASOS = NINGUNO."""
    texto = (get_config_valor("REGLA_DISCADO_CASOS") or "").strip() or _CASOS_DEFAULT
    return {c.strip().upper() for c in texto.split(",") if c.strip() and c.strip().upper() != "NINGUNO"}


def _base_del_lote(caso: str, descripcion_lote: str) -> int | None:
    """IDDATABASE donde entraron los contactos del lote (lo decide la Tarea
    de carga de Neotel, no nuestra config, que ya quedó desfasada antes)."""
    from app.core.sqlserver import get_db_name, sqlserver_cursor
    db = get_db_name(caso)
    linked = settings.sqlserver_linked_host
    with sqlserver_cursor("master") as cursor:
        cursor.execute(f"""
            SELECT TOP 1 d.IDDATABASE
            FROM [{linked}].[{db}].[dbo].[LOTES] l
            JOIN [{linked}].[{db}].[dbo].[CONTACTOS] c ON c.IDLOTE = l.IDLOTE
            JOIN [{linked}].[{db}].[dbo].[DB_CONTACTOS] d ON d.IDINTERNO = c.IDINTERNO
            WHERE l.DESCRIPCION = ?
        """, [descripcion_lote])
        row = cursor.fetchone()
    return int(row[0]) if row else None


def _avisar(titulo: str, mensaje: str, color: str) -> None:
    try:
        from app.core.ftp_watcher import _teams  # import diferido, evita ciclo de imports
        _teams(titulo=titulo, mensaje=mensaje, color=color, categoria=_CATEGORIA)
    except Exception as e:
        logger.warning("[ReglaDiscado] No se pudo notificar por Teams: %s", e)


def actualizar_si_corresponde(caso: str, descripcion_lote: str) -> None:
    """
    Llamar después de confirmar un lote en Neotel. Si `caso` está
    habilitado y la regla de la base del lote no es la de hoy, la cambia.
    No lanza excepción (no debe afectar la confirmación de la carga).
    """
    caso = (caso or "").upper()
    tipo = _TIPO_BASES.get(caso)
    if not tipo or caso not in casos_habilitados():
        return
    with _lock:  # varios lotes confirmándose a la vez no deben disparar la Tarea dos veces
        try:
            from app.core.sqlserver import obtener_hora_neotel
            from app.services.campanas_mensuales import obtener_base, regla_del_dia, _mismo_texto

            hora = obtener_hora_neotel()
            if not hora:
                return
            iddatabase = _base_del_lote(caso, descripcion_lote)
            if not iddatabase:
                logger.warning("[ReglaDiscado] %s: no se encontró la base del lote %s", caso, descripcion_lote)
                return

            esperada = regla_del_dia(hora.day)
            actual = (obtener_base(tipo, iddatabase) or {}).get("regla_discado", "")
            if not _mismo_texto(actual, esperada):
                _cambiar_regla(tipo, iddatabase, actual, esperada, descripcion_lote)
        except Exception as e:
            logger.exception("[ReglaDiscado] %s: error actualizando la regla", caso)
            _avisar(
                f"🛑 No se pudo actualizar la regla de discado de {tipo}",
                f"Lote: {descripcion_lote}<br>Error: {e}<br>Cambiarla a mano (pantalla Bases de la app o Neotel).",
                "DC3545",
            )
            return

        try:
            _reordenar_fila(caso, tipo, iddatabase, descripcion_lote)
        except Exception as e:
            logger.exception("[ReglaDiscado] %s: error reordenando la fila del discador", caso)
            _avisar(
                f"⚠️ No se pudo reordenar el discador de {tipo}",
                f"Base {iddatabase}, lote {descripcion_lote}<br>Error: {e}<br>"
                f"La carga nueva quedará al final de la fila. Para arreglarlo a mano: en la base de Neotel "
                f"cambiar el orden de discado y volverlo a IDINTERNO DESC.",
                "FFA500",
            )


def _cambiar_regla(tipo: str, iddatabase: int, actual: str, esperada: str, descripcion_lote: str) -> None:
    from app.services.campanas_mensuales import actualizar_regla
    r = actualizar_regla(tipo, iddatabase, esperada, usuario="sistema", omitir_verificacion=True)
    logger.info("[ReglaDiscado] %s: base %s, regla %s → %s", tipo, iddatabase, actual, r["regla_discado"])
    _avisar(
        f"✅ Regla de discado de {tipo} actualizada",
        f"Base {iddatabase}: <b>{actual or '(vacía)'}</b> → <b>{r['regla_discado']}</b><br>"
        f"Contactos abiertos para discar: {r['abiertos']}<br>"
        f"{r.get('respuesta_neotel') or ''}<br>"
        f"(con el primer lote del día: {descripcion_lote})",
        "28A745",
    )


# Posición que Neotel da a los contactos que llegan después de armar la fila.
_ORDEN_SIN_POSICION = 999999999


def _orden_alternado(orden: str) -> str | None:
    """Mismo orden con el último criterio invertido. Solo si el primero es
    IDINTERNO (único): los criterios siguientes nunca desempatan nada, así
    que el resultado es idéntico, pero Neotel lo ve como un cambio."""
    criterios = [c.strip() for c in (orden or "").split(",") if c.strip()]
    if not criterios or criterios[0].split()[0].upper() != "IDINTERNO" or len(criterios) < 2:
        return None
    campo, *direccion = criterios[-1].split()
    otra = "ASC" if (direccion[0].upper() if direccion else "ASC") == "DESC" else "DESC"
    return ", ".join(criterios[:-1] + [f"{campo} {otra}"])


def _sin_posicion(caso: str, iddatabase: int, descripcion_lote: str) -> int:
    """Contactos abiertos del lote que Neotel dejó al final de la fila."""
    from app.core.sqlserver import get_db_name, sqlserver_cursor
    db = get_db_name(caso)
    linked = settings.sqlserver_linked_host
    with sqlserver_cursor("master") as cursor:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM [{linked}].[{db}].[dbo].[LOTES] l
            JOIN [{linked}].[{db}].[dbo].[CONTACTOS] c ON c.IDLOTE = l.IDLOTE
            JOIN [{linked}].[{db}].[dbo].[DB_CONTACTOS] d ON d.IDINTERNO = c.IDINTERNO
            WHERE l.DESCRIPCION = ? AND d.IDDATABASE = ? AND d.GESTIONADO = 'N'
              AND ISNULL(d.ORDEN, {_ORDEN_SIN_POSICION}) = {_ORDEN_SIN_POSICION}
        """, [descripcion_lote, iddatabase])
        return int(cursor.fetchone()[0])


def _reordenar_fila(caso: str, tipo: str, iddatabase: int, descripcion_lote: str) -> None:
    """
    Neotel arma la fila del discador (DB_CONTACTOS.ORDEN, según el "orden de
    discado" de la base) solo cuando la base entra a la campaña o cuando se
    cambia ese orden. Lo cargado después queda con ORDEN 999999999, al final:
    el 02-11-2026 la carga de las 09:00 se llamó toda la tarde y las de las
    11:00 a 17:00 nunca. Volver a guardar la regla no la rearma (probado 4
    veces); cambiar el orden sí. Por eso, con cada lote, se guarda el orden
    alternando un criterio que no cambia nada (ver _orden_alternado), y
    Neotel vuelve a poner primero lo último cargado (IDINTERNO DESC).
    """
    from app.services.campanas_mensuales import obtener_base, actualizar_base

    pendientes = _sin_posicion(caso, iddatabase, descripcion_lote)
    if not pendientes:
        return
    base = obtener_base(tipo, iddatabase) or {}
    nuevo = _orden_alternado(base.get("orden_discado", ""))
    if not nuevo:
        logger.info("[ReglaDiscado] %s: base %s con orden %r, no se reordena", tipo, iddatabase, base.get("orden_discado"))
        return

    valores = {k: base[k] for k in (
        "estado", "diferencia_horaria", "permite_agregar", "intentos_discador", "autocerrar_contactos",
        "intentos_totales", "autocerrar_contactos_totales", "intentos_dia", "intentos_mes", "telefonos_a_discar",
    )}
    valores["orden_discado"] = nuevo
    actualizar_base(tipo, iddatabase, valores, usuario="sistema", omitir_verificacion=True)

    quedan = pendientes
    for _ in range(10):  # desde la pantalla de Neotel la fila se rearmó en segundos
        quedan = _sin_posicion(caso, iddatabase, descripcion_lote)
        if not quedan:
            break
        time.sleep(3)
    logger.info("[ReglaDiscado] %s: base %s, orden %r → %r; lote %s sin posición: %s → %s",
                tipo, iddatabase, base.get("orden_discado"), nuevo, descripcion_lote, pendientes, quedan)
    if quedan:
        raise RuntimeError(f"el orden se guardó pero {quedan} de {pendientes} contactos del lote siguen al final de la fila")
