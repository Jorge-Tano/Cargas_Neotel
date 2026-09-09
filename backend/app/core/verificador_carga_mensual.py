"""
verificador_carga_mensual.py
=============================
La base mensual operativa de PL y REFI (pantalla "Datos de la base de
datos" en Neotel) se crea sola cada mes vía el flujo interno de Neotel
(ECRM_001), alrededor del día 14-15 — pero con una configuración por
defecto que no es la que usamos (contactos a procesar, intentos de
discado, orden de discado, etc. quedan mal). Antes había que entrar a
mano a corregirla; este módulo la detecta y la corrige solo.

No es el mismo caso que app.core.verificador_iddatabase (que sigue el
catálogo "LEAKAGE" para saber qué IDDATABASE usar en las cargas diarias
de SAV/AV/REFI/PL) — acá la base se identifica por fecha de creación
(FHALTA), no por nombre: esta base se llama "BASE_DD-MM-AAAA", un
patrón sin relación con "BASE PL LEAKAGE ...".

Cómo funciona (mismo patrón de loop en background que los otros
verificadores, ver ftp_watcher.py / verificador_iddatabase.py):
  - Desde el día esperado en adelante, y mientras no se haya confirmado
    la corrección de este mes, busca la base creada desde el día
    anterior al esperado en adelante
    (campanas_mensuales.detectar_base_mensual_reciente).
  - Si la encuentra y su configuración no coincide con la plantilla
    correcta (campanas_mensuales.PLANTILLA_CORRECTA), la corrige
    (campanas_mensuales.actualizar_base) — para REFI además renombra
    la Descripción a "BASE RN {MES} {AÑO}"; PL conserva su Descripción
    tal cual (así lo pidió el usuario).
  - Si ya coincide, o ya se corrigió, se marca el mes como confirmado
    y — recién ahí, con la base ya en su configuración final — se
    dispara el export "Resultante" (FTP) de ese tipo
    (campanas_mensuales.ejecutar_resultante), una única Tarea de Neotel
    aparte de la 83 (dispatch por ClienteCod, igual patrón).
  - Con el Resultante ya disparado, TODO EL RESTO queda encadenado sin
    intervención humana ("todo debe quedar unido", pedido explícito):
    espera a que el archivo del Resultante aparezca en su carpeta
    (/UPLOAD/Resultante{PL,REFI}), busca el Excel mensual más reciente
    en el FTP principal, corre el cruce (carga_mensual.procesar_carga_*)
    y aplica el resultado en Neotel — Actualizar Datos + Eliminar por
    IDINTERNO (carga_mensual.aplicar_en_neotel). Ya no hay ningún paso
    de revisión humana en el medio.
  - Cualquier falla en esta cadena (Resultante no configurado/no
    dispara, archivo que no aparece a tiempo, no hay Excel mensual
    reciente, error al aplicar en Neotel) NO bloquea la confirmación
    del mes (la base ya quedó bien) — solo avisa por Teams. Para
    reintentar la cadena completa manualmente ante una falla, usar el
    botón "Probar verificación mensual" (forzar=True), que ignora el
    mes-ya-confirmado.
  - Si no se encuentra ninguna base todavía, o la corrección falla, se
    avisa por Teams (una vez por día de atraso) y se reintenta cada
    INTERVALO_PENDIENTE_SEG.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
import time
from datetime import date

from app.core.postgres import get_config_valor, set_config_global
from app.core.sqlserver import obtener_hora_neotel
from app.services.campanas_mensuales import (
    detectar_base_mensual_reciente,
    actualizar_base,
    ejecutar_resultante,
    PLANTILLA_CORRECTA,
)

logger = logging.getLogger(__name__)

INTERVALO_PENDIENTE_SEG = 15 * 60   # sondeo mientras hay una corrección pendiente
INTERVALO_OCIOSO_SEG    = 6 * 3600  # sin nada pendiente hoy, revisar de nuevo en

_TIPOS = ("PL", "REFI")

_DIA_CREACION_DEFAULT = {"PL": 15, "REFI": 15}

_MESES_ES = [
    "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE",
]

# Evita re-notificar "aún no se detectó" en cada sondeo de 15 min —
# solo una vez por día de atraso, por tipo.
_ultimo_aviso_pendiente: dict[str, date] = {}


def _hoy_neotel() -> date:
    """
    Fecha del día según el reloj del propio SQL Server de Neotel (hora
    de Chile, que es cuando corre la tarea que crea la base — 6:00 AM),
    no la de la máquina que corre este backend: mismo motivo que
    sqlserver.obtener_hora_neotel, evita desfases si el backend corriera
    en otra zona horaria. Si falla la consulta, cae a la hora local.
    """
    hora = obtener_hora_neotel()
    return hora.date() if hora else date.today()


def _dia_creacion(tipo: str) -> int:
    valor = get_config_valor(f"DIA_CREACION_CARGA_MENSUAL_{tipo}")
    try:
        return int(valor) if valor else _DIA_CREACION_DEFAULT[tipo]
    except (TypeError, ValueError):
        return _DIA_CREACION_DEFAULT[tipo]


def _mes_confirmado(tipo: str) -> str:
    return get_config_valor(f"CARGA_MENSUAL_{tipo}_MES_CONFIRMADO")


def _descripcion_refi(hoy: date) -> str:
    return f"BASE RN {_MESES_ES[hoy.month - 1]} {hoy.year}"


def _difiere(base: dict) -> bool:
    for campo, esperado in PLANTILLA_CORRECTA.items():
        actual = base.get(campo)
        if isinstance(esperado, bool):
            if bool(actual) != esperado:
                return True
        elif str(actual) != str(esperado):
            return True
    return False


def _esperar_txt_resultante(tipo: str, desde_ts: float, timeout_seg: int = 180, intervalo_seg: int = 10) -> str | None:
    """Sondea la carpeta del Resultante hasta que aparezca un archivo con
    mtime >= desde_ts (el que generó nuestro propio disparo)."""
    from app.core.ftp_neotel17 import encontrar_txt_resultante_reciente
    limite = time.time() + timeout_seg
    while time.time() < limite:
        ruta = encontrar_txt_resultante_reciente(tipo, desde=desde_ts)
        if ruta:
            return ruta
        time.sleep(intervalo_seg)
    return None


def _procesar_y_aplicar_automatico(tipo: str, iddatabase: int) -> dict:
    """
    Continúa la cadena después de disparar el Resultante, sin
    intervención humana: espera el TXT del Resultante, busca el Excel
    mensual más reciente en el FTP principal, corre el cruce
    (carga_mensual.procesar_carga_*) y aplica el resultado en Neotel
    (Actualizar Datos + Eliminar por IDINTERNO).
    """
    from app.core.ftp_neotel17 import descargar_archivo_ftp17, mtime_archivo_ftp17
    from app.core.ftp import encontrar_excel_mensual_reciente, descargar_archivo_sftp_ruta, mtime_archivo_sftp
    from app.services.carga_mensual import procesar_carga_pl, procesar_carga_refi, aplicar_en_neotel

    hora_disparo = time.time()
    ruta_txt = _esperar_txt_resultante(tipo, hora_disparo)
    if not ruta_txt:
        raise RuntimeError("El Resultante no generó archivo a tiempo (timeout de 3 min)")

    txt_bytes = descargar_archivo_ftp17(ruta_txt)
    txt_nombre = ruta_txt.rsplit("/", 1)[-1]
    mtime_txt = mtime_archivo_ftp17(ruta_txt)
    txt_identidad = (ruta_txt, mtime_txt) if mtime_txt else None

    ruta_excel = encontrar_excel_mensual_reciente(tipo)
    if not ruta_excel:
        raise RuntimeError("No se encontró Excel mensual reciente en el FTP principal")
    excel_bytes = descargar_archivo_sftp_ruta(ruta_excel)
    excel_nombre = ruta_excel.rsplit("/", 1)[-1]
    mtime_excel = mtime_archivo_sftp(ruta_excel)
    excel_identidad = (ruta_excel, mtime_excel) if mtime_excel else None

    fn = procesar_carga_pl if tipo == "PL" else procesar_carga_refi
    resultado = fn(
        txt_bytes, txt_nombre, excel_bytes, excel_nombre, tempfile.gettempdir(),
        txt_identidad=txt_identidad, excel_identidad=excel_identidad,
    )

    aplicar_en_neotel(
        tipo, iddatabase, resultado["archivos_update"], resultado["archivo_eliminar"],
        usuario="sistema",
    )
    return resultado


def _disparar_resultante(tipo: str, iddatabase: int) -> bool:
    """
    Dispara el Resultante y, si sale bien, encadena sin pausas el resto
    del proceso (ver _procesar_y_aplicar_automatico) — no lanza
    excepción en ningún punto de la cadena: cualquier falla solo avisa
    por Teams (no revierte la confirmación del mes: la base ya quedó
    bien, todo lo demás es un paso aparte que se puede reintentar a
    mano con el botón de prueba manual, forzar=True).
    """
    try:
        ok = ejecutar_resultante(tipo, iddatabase)
    except Exception as e:
        logger.exception("[VerificadorCargaMensual] Error disparando Resultante de %s", tipo)
        _avisar(
            f"🛑 Error disparando Resultante de {tipo}",
            f"IDDATABASE={iddatabase}<br>Error: {e}",
            "DC3545",
        )
        return False

    if not ok:
        logger.warning("[VerificadorCargaMensual] %s: no se pudo disparar el Resultante (IDDATABASE=%s)",
                        tipo, iddatabase)
        _avisar(
            f"⚠️ Resultante de {tipo} no disparado",
            f"IDDATABASE={iddatabase}<br>"
            f"Revisar ID_TAREA_RESULTANTE en Configuración, o el resultado de la Tarea en Neotel.",
            "FFA500",
        )
        return False

    try:
        resultado = _procesar_y_aplicar_automatico(tipo, iddatabase)
    except Exception as e:
        logger.exception("[VerificadorCargaMensual] Error en la cadena automática de %s", tipo)
        _avisar(
            f"🛑 Error en la carga mensual automática de {tipo}",
            f"IDDATABASE={iddatabase}<br>Error: {e}<br>"
            f"El Resultante ya se disparó — revisar manualmente desde acá en adelante "
            f"(Excel mensual, cruce, Actualizar Datos/Eliminar).",
            "DC3545",
        )
        return False

    logger.info("[VerificadorCargaMensual] %s: carga mensual aplicada en Neotel (IDDATABASE=%s)", tipo, iddatabase)
    _avisar(
        f"✅ Carga mensual de {tipo} aplicada en Neotel",
        f"IDDATABASE={iddatabase}<br>"
        f"Entrada: {resultado['total_entrada']} · Carga: {resultado['total_carga']} · "
        f"Eliminados: {resultado['total_eliminados']} · Sin cruce: {resultado['total_sin_cruce']}",
        "28A745",
    )
    return True


def _avisar(titulo: str, mensaje: str, color: str) -> None:
    try:
        from app.core.ftp_watcher import _teams  # import diferido, evita ciclo de imports
        _teams(titulo=titulo, mensaje=mensaje, color=color)
    except Exception as e:
        logger.warning("[VerificadorCargaMensual] No se pudo notificar por Teams: %s", e)


def verificar_tipo(tipo: str, hoy: date | None = None, forzar: bool = False) -> dict:
    """
    Revisa/corrige la base mensual de `tipo` (PL/REFI). No lanza
    excepción — cualquier error queda registrado en logs, avisado por
    Teams y devuelto en el dict de resultado.

    `forzar=True` ignora el mes-ya-confirmado y el día esperado — lo usa
    el botón de prueba manual (endpoint /carga-mensual/verificar) para
    poder probar el flujo completo sin esperar al día 15.

    Retorna {"pendiente": bool, "iddatabase": int|None, "corregido":
    bool|None, "resultante_ok": bool|None, "mensaje": str, "error":
    str|None}. "pendiente" es True si llegó el día esperado pero
    todavía no se pudo confirmar/corregir la base de este mes (el loop
    lo usa para decidir la frecuencia de sondeo). "resultante_ok" solo
    se completa cuando la base queda confirmada (ver
    campanas_mensuales.ejecutar_resultante) — None si ni siquiera se
    llegó a intentar.
    """
    hoy = hoy or _hoy_neotel()
    mes_actual = f"{hoy.year:04d}-{hoy.month:02d}"

    if not forzar and _mes_confirmado(tipo) == mes_actual:
        return {"pendiente": False, "iddatabase": None, "corregido": None, "resultante_ok": None,
                "mensaje": "Ya se confirmó la base de este mes.", "error": None}

    dia = _dia_creacion(tipo)
    if not forzar and hoy.day < dia:
        return {"pendiente": False, "iddatabase": None, "corregido": None, "resultante_ok": None,
                "mensaje": f"Aún no llega el día {dia}.", "error": None}

    desde = date(hoy.year, hoy.month, max(dia - 1, 1))
    base = detectar_base_mensual_reciente(tipo, desde)

    if not base:
        if _ultimo_aviso_pendiente.get(tipo) != hoy:
            _ultimo_aviso_pendiente[tipo] = hoy
            logger.warning("[VerificadorCargaMensual] %s: base de %s aún no detectada (esperada desde el día %d)",
                            tipo, mes_actual, dia)
            _avisar(
                f"⚠️ Base mensual de {tipo} aún no detectada",
                f"Se esperaba desde el día {dia} de {mes_actual}.<br>"
                f"Se reintentará cada 15 minutos hasta detectarla.",
                "FFA500",
            )
        return {"pendiente": True, "iddatabase": None, "corregido": None, "resultante_ok": None,
                "mensaje": "Base aún no detectada.", "error": None}

    if not _difiere(base):
        set_config_global({f"CARGA_MENSUAL_{tipo}_MES_CONFIRMADO": mes_actual})
        _ultimo_aviso_pendiente.pop(tipo, None)
        logger.info("[VerificadorCargaMensual] %s: base %s ya estaba correcta, mes confirmado",
                    tipo, base["iddatabase"])
        resultante_ok = _disparar_resultante(tipo, base["iddatabase"])
        return {"pendiente": False, "iddatabase": base["iddatabase"], "corregido": False,
                "resultante_ok": resultante_ok, "mensaje": "La base ya estaba correcta.", "error": None}

    nueva_descripcion = _descripcion_refi(hoy) if tipo == "REFI" else None
    try:
        actualizar_base(
            tipo, base["iddatabase"], PLANTILLA_CORRECTA, usuario="sistema",
            nueva_descripcion=nueva_descripcion, omitir_verificacion=True,
        )
    except Exception as e:
        logger.exception("[VerificadorCargaMensual] Error corrigiendo base %s de %s",
                          base["iddatabase"], tipo)
        if _ultimo_aviso_pendiente.get(tipo) != hoy:
            _ultimo_aviso_pendiente[tipo] = hoy
            _avisar(
                f"🛑 Error corrigiendo base mensual de {tipo}",
                f"IDDATABASE={base['iddatabase']}<br>Error: {e}",
                "DC3545",
            )
        return {"pendiente": True, "iddatabase": base["iddatabase"], "corregido": False, "resultante_ok": None,
                "mensaje": "Falló la corrección.", "error": str(e)}

    set_config_global({f"CARGA_MENSUAL_{tipo}_MES_CONFIRMADO": mes_actual})
    _ultimo_aviso_pendiente.pop(tipo, None)
    logger.info("[VerificadorCargaMensual] %s: base %s corregida, mes confirmado",
                tipo, base["iddatabase"])
    _avisar(
        f"✅ Base mensual de {tipo} corregida",
        f"IDDATABASE={base['iddatabase']}<br>Se ajustó la configuración a la plantilla estándar.",
        "28A745",
    )
    resultante_ok = _disparar_resultante(tipo, base["iddatabase"])
    return {"pendiente": False, "iddatabase": base["iddatabase"], "corregido": True,
            "resultante_ok": resultante_ok, "mensaje": "Base corregida.", "error": None}


def verificar_todos() -> bool:
    """
    Corre verificar_tipo para PL y REFI. Retorna True si alguno quedó
    pendiente — el loop usa esto para decidir si sondear de nuevo en 15
    min o dormir varias horas.
    """
    hoy = _hoy_neotel()
    pendiente = False
    for tipo in _TIPOS:
        try:
            if verificar_tipo(tipo, hoy)["pendiente"]:
                pendiente = True
        except Exception:
            logger.exception("[VerificadorCargaMensual] Error verificando %s", tipo)
    return pendiente


async def _loop_verificador():
    logger.info("[VerificadorCargaMensual] Loop iniciado.")
    while True:
        try:
            hay_pendientes = await asyncio.get_event_loop().run_in_executor(None, verificar_todos)
        except Exception:
            logger.exception("[VerificadorCargaMensual] Error en verificar_todos")
            hay_pendientes = False

        espera = INTERVALO_PENDIENTE_SEG if hay_pendientes else INTERVALO_OCIOSO_SEG
        logger.info("[VerificadorCargaMensual] Próxima corrida en %ds (%s)",
                    espera, "pendiente" if hay_pendientes else "sin pendientes")
        await asyncio.sleep(espera)


def arrancar_verificador_carga_mensual():
    logger.info("[VerificadorCargaMensual] Registrando tarea en event loop...")
    asyncio.ensure_future(_loop_verificador())
