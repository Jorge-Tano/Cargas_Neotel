"""
verificador_iddatabase.py
==========================
SAV, AV y REFI reciben una base/campaña nueva en Neotel el día 1 de
cada mes; PL la recibe el día 15. Antes había que entrar a la UI cada
vez y actualizar el IDDATABASE a mano — este módulo lo detecta solo.

MKT y Carrito Abandonado NO tienen una fecha de rotación conocida (no
hay ningún patrón mensual/quincenal documentado para ellos, a
diferencia de SAV/AV/REFI/PL), así que en vez de esperar un día fijo se
revisan en CADA pasada del loop: si el IDDATABASE con actividad más
reciente cambió respecto al configurado, se actualiza y se avisa —
sin asumir ninguna cadencia.

Cómo funciona (loop en background, mismo patrón que ftp_watcher.py):
  - La mayoría de los días no hay nada pendiente para SAV/AV/REFI/PL
    (aún no llega el día de creación, o ya se confirmó la base de este
    mes) → esos 4 casos no tocan SQL Server, solo comparan fechas.
  - Desde el día esperado en adelante, y mientras no se haya confirmado
    la base de ese mes, consulta Neotel (sqlserver.detectar_base_reciente)
    cada INTERVALO_PENDIENTE_SEG (15 min):
      - Si encuentra un IDDATABASE distinto al configurado → la base
        nueva ya se creó: actualiza config_global, marca el mes como
        confirmado y avisa por Teams. No se vuelve a consultar hasta
        el próximo mes.
      - Si sigue igual → todavía no se ha creado. Se avisa por Teams
        UNA vez por día de atraso (no en cada sondeo de 15 min) y se
        sigue reintentando.
  - MKT y CARRITO se consultan en cada pasada (cada 15 min si hay algo
    pendiente de los otros 4, si no cada INTERVALO_OCIOSO_SEG) y solo
    generan Teams/actualización cuando el IDDATABASE activo cambia.

Día de creación y umbral mínimo de registros configurables desde
config_global (DIA_CREACION_{CASO}, MIN_REGISTROS_BASE_NUEVA) por si
el proceso de negocio cambia sin tener que tocar código.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime

from app.core.postgres import get_config_valor, set_config_global
from app.core.sqlserver import detectar_base_reciente, _KEY_ID

logger = logging.getLogger(__name__)

INTERVALO_PENDIENTE_SEG = 15 * 60   # sondeo mientras hay una base pendiente
INTERVALO_OCIOSO_SEG    = 6 * 3600  # sin nada pendiente hoy, revisar de nuevo en

_DIA_CREACION_DEFAULT = {
    "SAV_AV": 1,
    "AV":     1,
    "REFI":   1,
    "PL":     15,
}
# Sin fecha de rotación conocida: se revisan en cada pasada del loop en
# vez de esperar un día del mes (ver detectar_base_reciente: usan patrón
# de TXTBASE, no TXTTIPOBASE).
_CASOS_SIN_FECHA = ["MKT", "CARRITO"]

_MIN_REGISTROS_DEFAULT = 50

# Evita re-notificar "aún no se ha creado" en cada sondeo de 15 min —
# solo una vez por día de atraso, por caso.
_ultimo_aviso_pendiente: dict[str, date] = {}


def _dia_creacion(caso: str) -> int:
    valor = get_config_valor(f"DIA_CREACION_{caso}")
    try:
        return int(valor) if valor else _DIA_CREACION_DEFAULT[caso]
    except (TypeError, ValueError):
        return _DIA_CREACION_DEFAULT[caso]


def _min_registros() -> int:
    valor = get_config_valor("MIN_REGISTROS_BASE_NUEVA")
    try:
        return int(valor) if valor else _MIN_REGISTROS_DEFAULT
    except (TypeError, ValueError):
        return _MIN_REGISTROS_DEFAULT


def _mes_confirmado(caso: str) -> str:
    return get_config_valor(f"IDDATABASE_{caso}_MES_CONFIRMADO")


def _notificar_nueva_base(caso: str, id_anterior: str, nuevo_id: int, cantidad: int) -> None:
    from app.core.ftp_watcher import _teams  # import diferido, evita ciclo de imports
    logger.info("[VerificadorID] %s: nueva base detectada %s → %s (%d registros)",
                caso, id_anterior, nuevo_id, cantidad)
    try:
        _teams(
            titulo=f"✅ Nueva base detectada — {caso}",
            mensaje=(
                f"IDDATABASE anterior: {id_anterior or '—'}<br>"
                f"IDDATABASE nuevo: {nuevo_id}<br>"
                f"Registros: {cantidad}<br>"
                f"Configuración actualizada automáticamente."
            ),
            color="28A745",
        )
    except Exception as e:
        logger.warning("[VerificadorID] No se pudo notificar por Teams: %s", e)


def verificar_caso(caso: str, hoy: date | None = None) -> bool:
    """
    Revisa si a `caso` (SAV_AV/AV/REFI/PL) le corresponde base nueva
    este mes y, si ya llegó el día esperado, consulta Neotel para
    confirmarla. No lanza excepción — cualquier error queda solo
    registrado en logs.

    Retorna True si el caso queda "pendiente" (llegó su día pero la
    base de este mes todavía no se confirmó) — así verificar_todos no
    necesita recalcular por su cuenta lo que esta función ya decidió.
    """
    hoy = hoy or date.today()
    mes_actual = f"{hoy.year:04d}-{hoy.month:02d}"

    if _mes_confirmado(caso) == mes_actual:
        return False  # ya se confirmó la base de este mes, nada que hacer

    dia = _dia_creacion(caso)
    if hoy.day < dia:
        return False  # aún no llega el día esperado

    encontrado = detectar_base_reciente(caso, min_registros=_min_registros())
    id_configurado = get_config_valor(_KEY_ID.get(caso, ""))
    id_configurado_int = int(id_configurado) if id_configurado else None

    if encontrado and encontrado[0] != id_configurado_int:
        nuevo_id, cantidad = encontrado
        set_config_global({
            _KEY_ID[caso]:                     str(nuevo_id),
            f"IDDATABASE_{caso}_MES_CONFIRMADO": mes_actual,
        })
        _ultimo_aviso_pendiente.pop(caso, None)
        _notificar_nueva_base(caso, id_configurado, nuevo_id, cantidad)
        return False  # ya quedó confirmada, no sigue pendiente

    # Todavía no se ha creado la base de este mes: avisar una vez por día
    if _ultimo_aviso_pendiente.get(caso) != hoy:
        _ultimo_aviso_pendiente[caso] = hoy
        logger.warning("[VerificadorID] %s: base de %s aún no creada (esperada desde el día %d)",
                        caso, mes_actual, dia)
        try:
            from app.core.ftp_watcher import _teams
            _teams(
                titulo=f"⚠️ Base de {caso} aún no creada",
                mensaje=(
                    f"Se esperaba desde el día {dia} de {mes_actual}.<br>"
                    f"IDDATABASE actual sigue siendo {id_configurado or '—'}.<br>"
                    f"Se reintentará cada 15 minutos hasta detectarla."
                ),
                color="FFA500",
            )
        except Exception as e:
            logger.warning("[VerificadorID] No se pudo notificar por Teams: %s", e)

    return True


def verificar_caso_sin_fecha(caso: str) -> None:
    """
    Para MKT/CARRITO (sin día de rotación conocido): compara el
    IDDATABASE configurado contra el que tiene actividad más reciente
    y, si difieren, actualiza config_global y avisa por Teams. Se llama
    en cada pasada del loop (no espera ninguna fecha) — si no hay
    cambio, no hace nada ni notifica.
    """
    encontrado = detectar_base_reciente(caso, min_registros=_min_registros())
    if not encontrado:
        return  # sin datos suficientes para comparar (o error de consulta)

    nuevo_id, cantidad = encontrado
    id_configurado = get_config_valor(_KEY_ID.get(caso, ""))
    id_configurado_int = int(id_configurado) if id_configurado else None

    if nuevo_id == id_configurado_int:
        return  # sigue igual, nada que hacer

    set_config_global({_KEY_ID[caso]: str(nuevo_id)})
    _notificar_nueva_base(caso, id_configurado, nuevo_id, cantidad)


def verificar_todos() -> bool:
    """
    Corre verificar_caso para SAV_AV/AV/REFI/PL, y verificar_caso_sin_fecha
    para MKT/CARRITO. Retorna True si algún caso mensual quedó pendiente
    (aún no confirmado pese a haber llegado su día) — el loop usa esto
    para decidir si sondear de nuevo en 15 min o dormir varias horas.
    """
    hoy = date.today()
    pendiente = False
    for caso in _DIA_CREACION_DEFAULT:
        try:
            if verificar_caso(caso, hoy):
                pendiente = True
        except Exception:
            logger.exception("[VerificadorID] Error verificando %s", caso)

    for caso in _CASOS_SIN_FECHA:
        try:
            verificar_caso_sin_fecha(caso)
        except Exception:
            logger.exception("[VerificadorID] Error verificando %s", caso)

    return pendiente


async def _loop_verificador():
    logger.info("[VerificadorID] Loop iniciado.")
    while True:
        try:
            hay_pendientes = await asyncio.get_event_loop().run_in_executor(None, verificar_todos)
        except Exception:
            logger.exception("[VerificadorID] Error en verificar_todos")
            hay_pendientes = False

        espera = INTERVALO_PENDIENTE_SEG if hay_pendientes else INTERVALO_OCIOSO_SEG
        logger.info("[VerificadorID] Próxima corrida en %ds (%s)",
                    espera, "pendiente" if hay_pendientes else "sin pendientes")
        await asyncio.sleep(espera)


def arrancar_verificador_iddatabase():
    logger.info("[VerificadorID] Registrando tarea en event loop...")
    asyncio.ensure_future(_loop_verificador())
