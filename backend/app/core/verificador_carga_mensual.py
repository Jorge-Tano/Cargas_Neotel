"""
verificador_carga_mensual.py
=============================
Carga mensual automática de PL y REFI (la base mensual operativa que
Neotel crea sola cada mes vía su flujo interno ECRM_001, alrededor del día
14-15, con una configuración por defecto que no es la que usamos).

No es el mismo caso que app.core.verificador_iddatabase (que sigue el
catálogo "LEAKAGE" para saber qué IDDATABASE usar en las cargas diarias
de SAV/AV/REFI/PL) — acá la base se identifica por fecha de creación
(FHALTA), no por nombre: esta base se llama "BASE_DD-MM-AAAA" (PL) /
"BASE RN {MES} {AÑO}" (REFI).

La carga se hace en PASOS que guardan su avance en config_global
(CARGA_MENSUAL_{TIPO}_AUTO, un JSON por mes), así una falla en cualquier
punto se reintenta desde ese paso en la siguiente pasada (cada 15 min),
sin repetir lo que ya quedó hecho:

  1. base       Detectar la base del mes y corregir su configuración
                (campanas_mensuales.PLANTILLA_CORRECTA; en REFI además la
                Descripción "BASE RN {MES SIGUIENTE} {AÑO}").
  2. llenado    Esperar a que Neotel termine de LLENAR la base: contactos
                > 0 y la misma cantidad en dos pasadas seguidas. El
                15-10-2026 la base REFI 99 se creó vacía a las 06:30 y se
                llenó recién a las 08:11 — pedir el Resultante antes dio un
                archivo vacío.
  3. excel      Que el Excel mensual de LiderBCI del mes que corresponde
                (el SIGUIENTE al día de creación: la base del 15/10 es la de
                noviembre) ya esté en el FTP principal.
  4. resultante Disparar el Resultante (Tarea ID_TAREA_RESULTANTE) y
                esperar el archivo NUEVO en /UPLOAD/Resultante{TIPO}:
                se compara contra lo que había ANTES de disparar (nombre y
                tamaño), no por hora — el FTP informa la hora sin segundos y
                la referencia se tomaba después de que la Tarea ya había
                terminado, así que el archivo nunca "aparecía" (causa de la
                falla del 15-10-2026 en PL y REFI). Se valida que traiga
                registros y que sean de ESTA base (columna "Bases de datos").
  5. cruce      Cruce TXT × Excel (carga_mensual.procesar_carga_*), que
                genera el Update y el eliminar.
  6. aplicar    Actualizar Datos + Eliminar en Neotel (Tarea
                ID_TAREA_ACTUALIZAR_DATOS) y CONFIRMAR CON DATOS: la base
                queda con la cantidad de "Carga" y esos contactos tienen el
                TXTBDD del Update. Si la Tarea no responde a tiempo, se
                confirma igual por los datos antes de darla por fallida.

Recién con el paso 6 confirmado se marca el mes
(CARGA_MENSUAL_{TIPO}_MES_CONFIRMADO). Antes se marcaba al corregir la
base, y una falla posterior dejaba el mes sin reintentos.

Cada falla avisa por Teams y se reintenta; tras _MAX_INTENTOS seguidos se
detiene y pide revisión manual. El botón "Probar verificación mensual"
(forzar=True) vuelve a empezar desde el paso 1 sin esperar el día ni el
llenado en dos pasadas — si la carga ya estaba aplicada, el paso 6 lo
detecta por los datos y no la vuelve a aplicar.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import threading
import time
from datetime import date

from app.core.postgres import get_config_valor, set_config_global
from app.core.sqlserver import obtener_hora_neotel
from app.services.campanas_mensuales import (
    detectar_base_mensual_reciente,
    actualizar_base,
    ejecutar_resultante,
    contar_contactos_base,
    contar_contactos_bdd,
    PLANTILLA_CORRECTA,
)

logger = logging.getLogger(__name__)

INTERVALO_PENDIENTE_SEG = 15 * 60   # sondeo mientras hay una carga pendiente
INTERVALO_OCIOSO_SEG    = 6 * 3600  # sin nada pendiente hoy, revisar de nuevo en

_TIPOS = ("PL", "REFI")

_DIA_CREACION_DEFAULT = {"PL": 15, "REFI": 15}

_MESES_ES = [
    "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE",
]

_CATEGORIA = "carga_mensual"  # Configuración → Notificaciones (app.core.notificaciones)
_MAX_INTENTOS = 8                  # ~2 h de reintentos cada 15 min antes de pedir revisión manual
_TIMEOUT_TAREA_SEG = 900           # espera a la respuesta del WebService en Tareas largas (81/82)
_ESPERA_RESULTANTE_SEG = 20 * 60   # tope esperando que aparezca el archivo del Resultante
_ESPERA_APLICADO_SEG = 20 * 60     # tope confirmando por datos que la carga quedó aplicada
_MIN_PROPORCION_RESULTANTE = 0.90  # el Resultante debe traer al menos este % de los contactos de la base

# Evita re-notificar lo mismo en cada sondeo de 15 min — una vez por día y motivo.
_ultimo_aviso: dict[str, date] = {}

# Una sola ejecución a la vez por tipo (loop automático vs. botón manual).
_locks = {t: threading.Lock() for t in _TIPOS}


def _hoy_neotel() -> date:
    """
    Fecha del día según el reloj del propio SQL Server de Neotel (hora
    de Chile, que es cuando corre la tarea que crea la base — 6:00 AM),
    no la de la máquina que corre este backend. Si falla la consulta,
    cae a la hora local.
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


def _mes_siguiente(hoy: date) -> tuple[int, int]:
    """(año, mes) del mes siguiente — la base creada el día 15 es la del mes que viene."""
    return (hoy.year + 1, 1) if hoy.month == 12 else (hoy.year, hoy.month + 1)


def _actualizar_iddatabase_agenda(tipo: str, iddatabase: int) -> None:
    """
    La "Agenda" de PL/REFI (usada por sqlserver.get_ruts_agendados para
    detectar resoluciones pendientes) apunta a la MISMA base mensual de
    campaña que administra este módulo. Si no se actualiza acá, queda
    pegada en la base del mes pasado (bug real del 15-09-2026).
    """
    from app.core.sqlserver import _AGENDA_KEY_ID
    clave = _AGENDA_KEY_ID.get(tipo)
    if clave:
        set_config_global({clave: str(iddatabase)})


def _descripcion_refi(hoy: date) -> str:
    """La base creada el día 15 es la del MES SIGUIENTE (la del 15 de
    octubre es la de noviembre)."""
    anio, mes = _mes_siguiente(hoy)
    return f"BASE RN {_MESES_ES[mes - 1]} {anio}"


def _difiere(base: dict) -> bool:
    for campo, esperado in PLANTILLA_CORRECTA.items():
        actual = base.get(campo)
        if isinstance(esperado, bool):
            if bool(actual) != esperado:
                return True
        elif str(actual) != str(esperado):
            return True
    return False


def _avisar(titulo: str, mensaje: str, color: str) -> None:
    try:
        from app.core.ftp_watcher import _teams  # import diferido, evita ciclo de imports
        _teams(titulo=titulo, mensaje=mensaje, color=color, categoria=_CATEGORIA)
    except Exception as e:
        logger.warning("[VerificadorCargaMensual] No se pudo notificar por Teams: %s", e)


def _avisar_una_vez(clave: str, hoy: date, titulo: str, mensaje: str, color: str) -> None:
    if _ultimo_aviso.get(clave) == hoy:
        return
    _ultimo_aviso[clave] = hoy
    _avisar(titulo, mensaje, color)


# ─────────────────────────────────────────────────────────────
# Estado persistido de la carga del mes
# ─────────────────────────────────────────────────────────────

def _clave_estado(tipo: str) -> str:
    return f"CARGA_MENSUAL_{tipo}_AUTO"


def _cargar_estado(tipo: str, mes: str) -> dict:
    """Estado de la carga de `mes` (AAAA-MM). Si lo guardado es de otro
    mes o está corrupto, empieza de cero."""
    try:
        estado = json.loads(get_config_valor(_clave_estado(tipo)) or "{}")
    except (TypeError, ValueError):
        estado = {}
    if estado.get("mes") != mes:
        estado = {"mes": mes, "paso": "base", "intentos": 0}
    return estado


def _guardar_estado(tipo: str, estado: dict) -> None:
    set_config_global({_clave_estado(tipo): json.dumps(estado, ensure_ascii=False)})


# ─────────────────────────────────────────────────────────────
# Pasos
# ─────────────────────────────────────────────────────────────

def _paso_excel(tipo: str, hoy: date) -> tuple[str, str]:
    """Ruta y nombre del Excel mensual del mes que corresponde, o
    RuntimeError("PENDIENTE: ...") si todavía no llega."""
    from app.core.ftp import encontrar_excel_mensual_reciente
    from app.services.carga_mensual import _mes_desde_nombre

    anio, mes = _mes_siguiente(hoy)
    esperado = f"{anio}{mes:02d}"
    ruta = encontrar_excel_mensual_reciente(tipo)
    if not ruta:
        raise RuntimeError(f"PENDIENTE: no se encontró ningún Excel mensual de {tipo} en el FTP principal")
    nombre = ruta.rsplit("/", 1)[-1]
    aaaamm, _mes_nombre = _mes_desde_nombre(nombre)
    if aaaamm != esperado:
        raise RuntimeError(
            f"PENDIENTE: el Excel mensual más reciente de {tipo} es {nombre} ({aaaamm}), "
            f"todavía no llega el de {esperado}"
        )
    return ruta, nombre


def _listar_resultantes(tipo: str) -> dict[str, int]:
    from app.core.ftp_neotel17 import listar_directorio_ftp17
    return {
        e["nombre"]: e["tamano"]
        for e in listar_directorio_ftp17(f"/UPLOAD/Resultante{tipo}")
        if not e["es_dir"]
    }


def _validar_resultante(txt_bytes: bytes, iddatabase: int, contactos_base: int) -> int:
    """Que el TXT traiga registros, que sean de ESTA base y que estén
    (casi) todos. Retorna la cantidad de registros."""
    from app.services.utils import leer_resolucion_txt

    df = leer_resolucion_txt(txt_bytes)
    filas = len(df)
    if filas == 0:
        raise RuntimeError("el Resultante salió vacío (solo encabezados)")
    if "Bases de datos" in df.columns:
        prefijo = f"{iddatabase} -"
        de_esta_base = df["Bases de datos"].astype(str).str.strip().str.startswith(prefijo).sum()
        if de_esta_base < filas * 0.95:
            raise RuntimeError(
                f"el Resultante no es de la base {iddatabase}: solo {de_esta_base} de {filas} registros lo son"
            )
    if contactos_base and filas < contactos_base * _MIN_PROPORCION_RESULTANTE:
        raise RuntimeError(
            f"el Resultante trae {filas} registros y la base tiene {contactos_base}: parece incompleto"
        )
    return filas


def _paso_resultante(tipo: str, iddatabase: int, contactos_base: int) -> str:
    """Dispara el Resultante y retorna la ruta del archivo NUEVO ya validado."""
    from app.core.ftp_neotel17 import descargar_archivo_ftp17

    carpeta = f"/UPLOAD/Resultante{tipo}"
    antes = _listar_resultantes(tipo)

    # La respuesta del WebService no decide nada: si tarda más que el
    # timeout, Neotel igual termina la Tarea — lo que cuenta es el archivo.
    respondio = ejecutar_resultante(tipo, iddatabase, timeout=_TIMEOUT_TAREA_SEG)
    if not respondio:
        logger.warning("[VerificadorCargaMensual] %s: la Tarea del Resultante no respondió OK, se espera el archivo igual", tipo)

    limite = time.time() + _ESPERA_RESULTANTE_SEG
    tamanos_previos: dict[str, int] = {}
    while time.time() < limite:
        ahora = _listar_resultantes(tipo)
        nuevos = {n: t for n, t in ahora.items() if t > 0 and antes.get(n) != t}
        # Estable = mismo tamaño que en la vuelta anterior (Neotel terminó de escribirlo)
        listos = [n for n, t in nuevos.items() if tamanos_previos.get(n) == t]
        if listos:
            nombre = max(listos, key=lambda n: (nuevos[n], n))
            ruta = f"{carpeta}/{nombre}"
            filas = _validar_resultante(descargar_archivo_ftp17(ruta), iddatabase, contactos_base)
            logger.info("[VerificadorCargaMensual] %s: Resultante %s OK (%d registros)", tipo, ruta, filas)
            return ruta
        tamanos_previos = nuevos
        time.sleep(20)

    raise RuntimeError(
        f"el Resultante no generó un archivo nuevo en {carpeta} en {_ESPERA_RESULTANTE_SEG // 60} min"
        + ("" if respondio else " (la Tarea tampoco respondió OK: revisar ID_TAREA_RESULTANTE en Configuración)")
    )


def _paso_cruce(tipo: str, ruta_txt: str, ruta_excel: str, mes: str) -> dict:
    from app.core.ftp_neotel17 import descargar_archivo_ftp17
    from app.core.ftp import descargar_archivo_sftp_ruta
    from app.services.carga_mensual import procesar_carga_pl, procesar_carga_refi

    salida = os.path.join(tempfile.gettempdir(), "carga_mensual_auto", tipo, mes)
    os.makedirs(salida, exist_ok=True)

    fn = procesar_carga_pl if tipo == "PL" else procesar_carga_refi
    # Sin "identidad" de archivos: el control de "archivo ya usado" es para
    # el proceso manual; acá un reintento con el mismo Excel es legítimo.
    resultado = fn(
        descargar_archivo_ftp17(ruta_txt), ruta_txt.rsplit("/", 1)[-1],
        descargar_archivo_sftp_ruta(ruta_excel), ruta_excel.rsplit("/", 1)[-1],
        salida,
    )
    if not resultado.get("archivos_update"):
        raise RuntimeError("el cruce no generó Update (0 registros para cargar)")
    return resultado


def _bdd_esperado(tipo: str, aaaamm: str) -> str:
    from app.services.carga_mensual import MESES_ES
    return f"Base {'PL' if tipo == 'PL' else 'RN'} {MESES_ES[int(aaaamm[4:6])]}"


def _aplicado(tipo: str, iddatabase: int, total_carga: int, bdd: str) -> tuple[bool, str]:
    """La carga quedó aplicada si la base tiene ~la cantidad de "Carga"
    (eliminación hecha) y esos contactos tienen el TXTBDD del Update."""
    total = contar_contactos_base(tipo, iddatabase)
    con_bdd = contar_contactos_bdd(tipo, iddatabase, bdd)
    tolerancia = max(50, int(total_carga * 0.005))  # contactos que entran por la operación del día
    ok = abs(total - total_carga) <= tolerancia and con_bdd >= total_carga - tolerancia
    return ok, f"base {iddatabase}: {total} contactos, {con_bdd} con '{bdd}' (esperado {total_carga})"


def _paso_aplicar(tipo: str, iddatabase: int, estado: dict) -> str:
    from app.services.carga_mensual import aplicar_en_neotel

    total_carga = estado["total_carga"]
    bdd = _bdd_esperado(tipo, estado["aaaamm"])

    ok, detalle = _aplicado(tipo, iddatabase, total_carga, bdd)
    if ok:
        return f"ya estaba aplicada ({detalle})"

    for ruta in [estado["archivo_eliminar"], *estado["archivos_update"]]:
        if not os.path.exists(ruta):
            raise RuntimeError(f"REHACER_CRUCE: no existe {ruta}")

    error_tarea = None
    try:
        aplicar_en_neotel(
            tipo, iddatabase, estado["archivos_update"], estado["archivo_eliminar"],
            usuario="sistema", timeout=_TIMEOUT_TAREA_SEG,
        )
    except Exception as e:
        # Puede ser solo que la Tarea no respondió a tiempo: se confirma por datos.
        error_tarea = str(e)
        logger.warning("[VerificadorCargaMensual] %s: la Tarea de aplicar respondió con error (%s), se confirma por datos", tipo, e)

    limite = time.time() + _ESPERA_APLICADO_SEG
    while True:
        ok, detalle = _aplicado(tipo, iddatabase, total_carga, bdd)
        if ok:
            return detalle
        if time.time() >= limite:
            raise RuntimeError(
                f"no se pudo confirmar que la carga quedó aplicada ({detalle})"
                + (f"; la Tarea respondió: {error_tarea}" if error_tarea else "")
            )
        time.sleep(60)


# ─────────────────────────────────────────────────────────────
# Orquestación
# ─────────────────────────────────────────────────────────────

def _resultado(pendiente: bool, estado: dict, mensaje: str, error: str | None = None) -> dict:
    return {
        "pendiente": pendiente,
        "iddatabase": estado.get("iddatabase"),
        "corregido": estado.get("corregido"),
        "resultante_ok": bool(estado.get("ruta_txt")),
        "paso": estado.get("paso"),
        "mensaje": mensaje,
        "error": error,
    }


def verificar_tipo(tipo: str, hoy: date | None = None, forzar: bool = False) -> dict:
    """
    Avanza la carga mensual de `tipo` (PL/REFI) desde el paso en que
    quedó. No lanza excepción: cualquier error queda en logs, se avisa
    por Teams y se devuelve en el dict ("pendiente" True → el loop vuelve
    a intentar en 15 min).
    """
    lock = _locks[tipo]
    if not lock.acquire(blocking=False):
        return {"pendiente": True, "iddatabase": None, "corregido": None, "resultante_ok": None,
                "paso": None, "mensaje": "Ya hay una carga mensual de este tipo en curso.", "error": None}
    try:
        return _verificar_tipo(tipo, hoy or _hoy_neotel(), forzar)
    finally:
        lock.release()


def _verificar_tipo(tipo: str, hoy: date, forzar: bool) -> dict:
    mes_actual = f"{hoy.year:04d}-{hoy.month:02d}"

    if not forzar and _mes_confirmado(tipo) == mes_actual:
        return {"pendiente": False, "iddatabase": None, "corregido": None, "resultante_ok": None,
                "paso": "aplicado", "mensaje": "La carga mensual de este mes ya está aplicada.", "error": None}

    dia = _dia_creacion(tipo)
    if not forzar and hoy.day < dia:
        return {"pendiente": False, "iddatabase": None, "corregido": None, "resultante_ok": None,
                "paso": None, "mensaje": f"Aún no llega el día {dia}.", "error": None}

    estado = {"mes": mes_actual, "paso": "base", "intentos": 0} if forzar else _cargar_estado(tipo, mes_actual)
    if estado["paso"] == "detenido":
        return _resultado(False, estado, "Detenida tras varios intentos fallidos: requiere revisión manual "
                                         "(usar 'Probar verificación mensual' para reintentar).", estado.get("ultimo_error"))

    try:
        # 1. Base del mes, con la configuración correcta
        base = detectar_base_mensual_reciente(tipo, date(hoy.year, hoy.month, max(dia - 1, 1)))
        if not base:
            _avisar_una_vez(f"{tipo}-base", hoy, f"⚠️ Base mensual de {tipo} aún no creada",
                            f"Se esperaba desde el día {dia} de {mes_actual}.<br>Se reintenta cada 15 minutos.",
                            "FFA500")
            return _resultado(True, estado, "Base del mes aún no creada en Neotel.")
        iddatabase = base["iddatabase"]
        if estado.get("iddatabase") not in (None, iddatabase):
            # Apareció otra base distinta a la que se venía trabajando: empezar de nuevo con esa.
            estado = {"mes": mes_actual, "paso": "base", "intentos": 0}
        estado["iddatabase"] = iddatabase

        if estado["paso"] == "base":
            if _difiere(base):
                actualizar_base(
                    tipo, iddatabase, PLANTILLA_CORRECTA, usuario="sistema",
                    nueva_descripcion=_descripcion_refi(hoy) if tipo == "REFI" else None,
                    omitir_verificacion=True,
                )
                estado["corregido"] = True
                logger.info("[VerificadorCargaMensual] %s: base %s corregida", tipo, iddatabase)
            else:
                estado["corregido"] = False
            _actualizar_iddatabase_agenda(tipo, iddatabase)
            estado["paso"] = "llenado"
            _guardar_estado(tipo, estado)

        # 2. Base llena y estable
        if estado["paso"] == "llenado":
            contactos = contar_contactos_base(tipo, iddatabase)
            if forzar and contactos > 0:
                time.sleep(60)
                estable = contar_contactos_base(tipo, iddatabase) == contactos
            else:
                # "Igual" con tolerancia mínima: durante el día entra algún
                # contacto suelto (el 15-10 la 99 pasó de 93.042 a 93.043),
                # mientras que el llenado real es un salto de miles.
                previos = estado.get("contactos_previos") or 0
                estable = contactos > 0 and previos > 0 and abs(contactos - previos) <= max(5, contactos // 1000)
            if not estable:
                estado["contactos_previos"] = contactos
                _guardar_estado(tipo, estado)
                return _resultado(True, estado, f"Esperando que Neotel termine de llenar la base {iddatabase} "
                                                f"({contactos} contactos por ahora).")
            estado["contactos_base"] = contactos
            estado["paso"] = "excel"
            _guardar_estado(tipo, estado)

        # 3. Excel mensual del mes que corresponde
        if estado["paso"] == "excel":
            estado["ruta_excel"], _nombre = _paso_excel(tipo, hoy)
            estado["paso"] = "resultante"
            _guardar_estado(tipo, estado)

        # 4. Resultante nuevo y válido
        if estado["paso"] == "resultante":
            estado["ruta_txt"] = _paso_resultante(tipo, iddatabase, estado["contactos_base"])
            estado["paso"] = "cruce"
            _guardar_estado(tipo, estado)

        # 5. Cruce TXT × Excel
        if estado["paso"] == "cruce":
            r = _paso_cruce(tipo, estado["ruta_txt"], estado["ruta_excel"], mes_actual)
            estado.update({
                "archivos_update": r["archivos_update"], "archivo_eliminar": r["archivo_eliminar"],
                "aaaamm": r["aaaamm"], "total_entrada": r["total_entrada"], "total_carga": r["total_carga"],
                "total_eliminados": r["total_eliminados"], "total_sin_cruce": r["total_sin_cruce"],
            })
            estado["paso"] = "aplicar"
            _guardar_estado(tipo, estado)

        # 6. Aplicar en Neotel y confirmar con datos
        if estado["paso"] == "aplicar":
            try:
                detalle = _paso_aplicar(tipo, iddatabase, estado)
            except RuntimeError as e:
                if str(e).startswith("REHACER_CRUCE"):  # los archivos se perdieron (ej. reinicio): rehacer el cruce
                    estado["paso"] = "cruce"
                    _guardar_estado(tipo, estado)
                raise
            estado["paso"] = "aplicado"
            estado["intentos"] = 0
            estado.pop("ultimo_error", None)
            _guardar_estado(tipo, estado)
            set_config_global({f"CARGA_MENSUAL_{tipo}_MES_CONFIRMADO": mes_actual})
            logger.info("[VerificadorCargaMensual] %s: carga mensual aplicada — %s", tipo, detalle)
            _avisar(
                f"✅ Carga mensual de {tipo} aplicada en Neotel",
                f"IDDATABASE={iddatabase}<br>"
                f"Entrada: {estado['total_entrada']} · Carga: {estado['total_carga']} · "
                f"Eliminados: {estado['total_eliminados']} · Sin cruce: {estado['total_sin_cruce']}<br>"
                f"Verificado: {detalle}",
                "28A745",
            )
            return _resultado(False, estado, f"Carga mensual aplicada ({detalle}).")

        return _resultado(False, estado, "Sin acciones pendientes.")

    except Exception as e:
        mensaje = str(e)
        if mensaje.startswith("PENDIENTE: "):
            # Algo que todavía no está (ej. el Excel del mes): no es falla, se espera.
            mensaje = mensaje[len("PENDIENTE: "):]
            _avisar_una_vez(f"{tipo}-espera", hoy, f"⏳ Carga mensual de {tipo} en espera", mensaje, "FFA500")
            return _resultado(True, estado, mensaje)

        logger.exception("[VerificadorCargaMensual] %s: falla en el paso '%s'", tipo, estado.get("paso"))
        estado["intentos"] = estado.get("intentos", 0) + 1
        estado["ultimo_error"] = mensaje
        if estado["intentos"] >= _MAX_INTENTOS:
            estado["paso_fallido"] = estado.get("paso")
            estado["paso"] = "detenido"
            _guardar_estado(tipo, estado)
            _avisar(
                f"🛑 Carga mensual de {tipo} detenida: requiere revisión manual",
                f"Paso: {estado['paso_fallido']} · IDDATABASE={estado.get('iddatabase')}<br>"
                f"Falló {estado['intentos']} veces seguidas. Último error: {mensaje}",
                "DC3545",
            )
            return _resultado(False, estado, "Detenida: requiere revisión manual.", mensaje)

        _guardar_estado(tipo, estado)
        _avisar(
            f"⚠️ Carga mensual de {tipo}: falla en el paso '{estado.get('paso')}' "
            f"(intento {estado['intentos']}/{_MAX_INTENTOS})",
            f"IDDATABASE={estado.get('iddatabase')}<br>Error: {mensaje}<br>Se reintenta en 15 minutos.",
            "FFA500",
        )
        return _resultado(True, estado, f"Falla en el paso '{estado.get('paso')}', se reintenta.", mensaje)


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
        logger.debug("[VerificadorCargaMensual] Próxima corrida en %ds", espera)
        await asyncio.sleep(espera)


def arrancar_verificador_carga_mensual():
    logger.info("[VerificadorCargaMensual] Registrando tarea en event loop...")
    asyncio.ensure_future(_loop_verificador())
