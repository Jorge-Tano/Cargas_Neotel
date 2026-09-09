"""
confirmacion_carga.py
======================
Confirma que una carga recién subida a Neotel (por FTP, ver
ftp_neotel17.subir_archivo_carga_txt) efectivamente quedó insertada en
su base de datos SQL Server — cruzando los valores del lote contra
CONTACTOS/DB_CONTACTOS (mismo mecanismo que sqlserver.get_repetidos,
pero DESPUÉS de subir en vez de antes). Por defecto se cruza por RUT;
MKT (que no tiene RUT en su Excel de origen) se cruza por Patente — ver
parámetro `columna`.

Se llama al final de cada procesador (SAV, AV, REFI, PL, MKT, CARRITO)
vía confirmar_carga_en_segundo_plano. Por defecto, Neotel importa lo
subido en un proceso propio que corre una vez por hora, a la hora en
punto y 15 minutos (xx:15) — así que confirmar a los pocos segundos de
subir siempre daría 0 (el import de Neotel ni siquiera habría corrido
todavía). Para ese caso (`carga_forzada=False`), se espera hasta el
próximo xx:16 (un minuto de margen tras el import) y se consulta UNA
sola vez — si a esa hora no aparece confirmado, se avisa y no se vuelve
a reintentar (Neotel no va a volver a importar hasta el próximo xx:15
de todos modos).

Cuando el procesador logra disparar el import al instante vía el
WebService de Neotel (app.core.neotel_ws.ejecutar_tarea — confirmado
síncrono: responde solo cuando el import ya insertó los registros),
pasa `carga_forzada=True` junto con `hora_disparo` (hora real de Neotel,
capturada justo antes de disparar) y acá se confirma consultando
directo la tabla LOTES de Neotel (sqlserver.buscar_lote_reciente):
se busca el lote más nuevo de ese caso creado desde `hora_disparo` en
adelante, reintentando cada pocos segundos hasta un tope — y se compara
su columna REGISTROS contra lo que subimos. Es más preciso que cruzar
RUT/Patente contra CONTACTOS (que puede dar falsos positivos con datos
preexistentes no relacionados, o falsos negativos si Neotel guarda el
dato distinto a como lo esperamos — ambos casos ya se dieron en pruebas).

Corre en un hilo aparte (no en el pool de workers que procesa las
cargas): la espera hasta xx:16 puede ser de hasta ~1 hora, y bloquear
uno de los pocos workers de ese pool todo ese tiempo retrasaría el
procesamiento de otras cargas que compiten por el mismo pool mientras
esta solo espera. El procesador ya terminó y devolvió su resultado
antes de que esto corra — la confirmación llega después, solo por
Postgres (log_confirmacion_carga) siempre, y por Teams (Alertas Leakage,
mismo canal donde ya caen los avisos de ftp_watcher.py) SOLO cuando algo
no se pudo confirmar o falló — si confirma bien, no se notifica (a
pedido explícito: ese canal es para errores/pendientes, no para avisar
que algo salió bien).
"""

import threading
import time
from datetime import datetime, timedelta

from app.core.sqlserver import get_ruts_cargados, buscar_lote_reciente
from app.core.postgres import registrar_confirmacion_carga, get_config_valor

_MINUTO_OBJETIVO_DEFAULT = 16  # Neotel importa a xx:15; se consulta a xx:16
_TIMEOUT_LOTE_SEG = 40    # tope esperando a que aparezca el lote en Neotel
_INTERVALO_LOTE_SEG = 3   # cada cuánto se reintenta la consulta a LOTES

# "Alertas Leakage" es solo para los casos automáticos (los que corre el
# ftp_watcher solo); los casos manuales (PERDIDAS, AMALIA, OP_PERDIDAS,
# OP_WHATSAPP) los dispara Jorge desde la UI y ya ve el resultado ahí —
# no deben mandar nada a Teams, ni siquiera cuando no se confirman.
_CASOS_LEAKAGE = {"SAV_AV", "SAV", "AV", "REFI", "PL", "CARRITO", "MKT"}


def _config_numero(clave: str, default: float) -> float:
    valor = get_config_valor(clave)
    try:
        return float(valor) if valor else default
    except (TypeError, ValueError):
        return default


def _segundos_hasta_proximo_minuto(minuto_objetivo: int) -> float:
    """Segundos desde ahora hasta la próxima vez que el reloj marque el minuto `minuto_objetivo` de cualquier hora."""
    ahora = datetime.now()
    objetivo = ahora.replace(minute=minuto_objetivo, second=0, microsecond=0)
    if objetivo <= ahora:
        objetivo += timedelta(hours=1)
    return (objetivo - ahora).total_seconds()


def _confirmar_via_lote(caso: str, total: int, hora_disparo: datetime, emit) -> dict:
    """Confirma consultando LOTES directo — ver docstring del módulo."""
    intentos = max(1, int(_TIMEOUT_LOTE_SEG / _INTERVALO_LOTE_SEG))
    lote = None
    for intento in range(intentos):
        lote = buscar_lote_reciente(caso, hora_disparo)
        if lote:
            break
        emit(f"Esperando a que aparezca el lote en Neotel (intento {intento + 1}/{intentos})")
        time.sleep(_INTERVALO_LOTE_SEG)

    if lote is None:
        return {
            "caso": caso, "total_carga": total, "total_confirmado": None,
            "total_faltante": None, "confirmado": False, "faltantes": [],
            "metodo": "lote", "lote_descripcion": None,
        }

    registros = lote["registros"] or 0
    return {
        "caso":              caso,
        "total_carga":       total,
        "total_confirmado":  min(registros, total),
        "total_faltante":    max(total - registros, 0),
        "confirmado":        registros >= total,
        "faltantes":         [],
        "metodo":            "lote",
        "lote_descripcion":  lote["descripcion"],
    }


def _confirmar_via_presencia(caso: str, valores_limpios: set[str], columna: str, emit) -> dict:
    """Respaldo: espera hasta el próximo xx:MINUTO y cruza RUT/Patente
    contra CONTACTOS — se usa solo cuando no se pudo forzar el import
    (por eso no hay `hora_disparo` con el que buscar en LOTES)."""
    total = len(valores_limpios)
    minuto_objetivo = int(_config_numero("CONFIRMACION_MINUTO_OBJETIVO", _MINUTO_OBJETIVO_DEFAULT))
    espera = _segundos_hasta_proximo_minuto(minuto_objetivo)
    emit(f"Esperando hasta xx:{minuto_objetivo:02d} para confirmar en Neotel ({espera:.0f}s)")
    time.sleep(espera)

    try:
        confirmados = get_ruts_cargados(caso, sorted(valores_limpios), columna=columna)
    except Exception as e:
        print(f"[confirmar_carga] Error consultando {caso}: {e}")
        confirmados = set()

    faltantes = sorted(valores_limpios - confirmados)
    return {
        "caso":              caso,
        "total_carga":       total,
        "total_confirmado":  len(confirmados),
        "total_faltante":    len(faltantes),
        "confirmado":        len(faltantes) == 0,
        "faltantes":         faltantes[:50],  # cap para no inflar log/notificación
        "metodo":            "presencia",
        "lote_descripcion":  None,
    }


def confirmar_carga(
    caso: str,
    valores: list[str],
    columna: str = "TXTRUT",
    archivo_origen: str = "",
    usuario: str = "",
    carga_forzada: bool = False,
    hora_disparo: datetime | None = None,
    progress_cb=None,
) -> dict:
    """
    Confirma que `valores` (el lote recién cargado) quedó insertado en
    Neotel para `caso` (mismo identificador que usa sqlserver.get_repetidos:
    SAV_AV, AV, PL, REFI, CARRITO, MKT).

    Si `carga_forzada` es True y viene `hora_disparo`, confirma vía LOTES
    (_confirmar_via_lote — ver docstring del módulo). Si no, cae al
    respaldo por presencia de RUT/Patente en CONTACTOS
    (_confirmar_via_presencia, comparando por `columna`: TXTRUT por
    defecto, MKT usa TXTPATENTE), esperando hasta el próximo xx:MINUTO.

    Retorna {caso, total_carga, total_confirmado, total_faltante,
    confirmado, faltantes, metodo, lote_descripcion} y no lanza
    excepción: los errores de conexión/consulta a SQL Server quedan
    contenidos para que un caso mal configurado no tumbe esta función a
    mitad de camino — solo se registra el error.
    """
    def emit(msg):
        if progress_cb:
            progress_cb(msg)

    valores_limpios = {str(v).strip() for v in valores if str(v).strip()}
    total = len(valores_limpios)

    if total == 0:
        return {
            "caso": caso, "total_carga": 0, "total_confirmado": 0,
            "total_faltante": 0, "confirmado": True, "faltantes": [],
            "metodo": None, "lote_descripcion": None,
        }

    if carga_forzada and hora_disparo:
        emit("Carga forzada vía WebService Neotel, confirmando vía LOTES")
        resultado = _confirmar_via_lote(caso, total, hora_disparo, emit)
    else:
        resultado = _confirmar_via_presencia(caso, valores_limpios, columna, emit)

    confirmados_n = resultado["total_confirmado"]
    faltantes_n = resultado["total_faltante"]

    try:
        registrar_confirmacion_carga(
            tipo_caso=caso,
            total_carga=total,
            total_confirmado=confirmados_n or 0,
            confirmado=resultado["confirmado"],
            archivo_origen=archivo_origen,
            usuario=usuario,
        )
    except Exception as e:
        print(f"[confirmar_carga] No se pudo registrar en Postgres: {e}")

    if resultado["confirmado"]:
        # Éxito: no se notifica — Alertas Leakage es solo para errores o
        # cargas que no se pudieron confirmar (a pedido explícito).
        emit(f"✅ Carga confirmada en Neotel: {confirmados_n}/{total} registros")
    elif resultado["metodo"] == "lote" and resultado["lote_descripcion"] is None:
        emit(f"⚠️ No se encontró el lote de esta carga en Neotel tras {_TIMEOUT_LOTE_SEG}s")
        if caso in _CASOS_LEAKAGE:
            try:
                from app.core.ftp_watcher import _teams
                _teams(
                    titulo=f"⚠️ Carga {caso}: no se encontró el lote en Neotel",
                    mensaje=(
                        f"Archivo: {archivo_origen or '—'}<br>"
                        f"Registros subidos: {total}<br>"
                        f"Se disparó el import en Neotel pero no apareció ningún lote nuevo "
                        f"tras {_TIMEOUT_LOTE_SEG}s — revisar si el WebService realmente corrió."
                    ),
                    color="FFA500",
                )
            except Exception as e:
                print(f"[confirmar_carga] No se pudo notificar por Teams: {e}")
    else:
        emit(f"⚠️ Confirmación incompleta: {confirmados_n}/{total} registros en BD Neotel")
        if caso in _CASOS_LEAKAGE:
            try:
                from app.core.ftp_watcher import _teams
                detalle_lote = f"<br>Lote: {resultado['lote_descripcion']}" if resultado["metodo"] == "lote" else ""
                _teams(
                    titulo=f"⚠️ Carga {caso} no confirmada en BD Neotel",
                    mensaje=(
                        f"Archivo: {archivo_origen or '—'}<br>"
                        f"Registros subidos: {total}<br>"
                        f"Registros confirmados en BD: {confirmados_n}<br>"
                        f"Registros faltantes: {faltantes_n}"
                        f"{detalle_lote}"
                    ),
                    color="FFA500",
                )
            except Exception as e:
                print(f"[confirmar_carga] No se pudo notificar por Teams: {e}")

    return resultado


def confirmar_carga_en_segundo_plano(
    caso: str,
    valores: list[str],
    columna: str = "TXTRUT",
    archivo_origen: str = "",
    usuario: str = "",
    carga_forzada: bool = False,
    hora_disparo: datetime | None = None,
) -> None:
    """
    Lanza confirmar_carga en un hilo daemon aparte y retorna de
    inmediato, sin esperar el resultado. Pensada para llamarse justo
    después de que un procesador (mkt.py, carrito_abandonado.py,
    refi_pl.py, sav_av.py) ya terminó y devolvió su resultado — la
    espera hasta xx:16 puede ser de hasta ~1 hora, y no tiene sentido
    que el caller (ni el worker del pool que lo procesó) se quede
    esperando eso.

    No hay valor de retorno sincrónico: el resultado queda en Postgres
    (postgres.get_confirmaciones_carga) y, si algo no confirma, se
    notifica por Teams — igual que si se hubiera llamado de forma
    síncrona.
    """
    hilo = threading.Thread(
        target=confirmar_carga,
        kwargs=dict(
            caso=caso, valores=valores, columna=columna,
            archivo_origen=archivo_origen, usuario=usuario,
            carga_forzada=carga_forzada, hora_disparo=hora_disparo,
        ),
        daemon=True,
        name=f"confirmar_carga-{caso}",
    )
    hilo.start()
