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
vía confirmar_carga_en_segundo_plano: espera un tiempo — Neotel demora
en procesar el archivo subido — y reintenta unas cuantas veces antes de
darse por vencido, con el mismo patrón de espera/reintento que ya se usa
para descargar_archivo_sftp_por_nombre. Cada reintento solo vuelve a
consultar lo que sigue faltando (no el lote completo), y lo confirmado
en un intento anterior nunca se pierde aunque un intento posterior falle.

Corre en un hilo aparte (no en el pool de workers que procesa las
cargas): puede tardar hasta ~90s en sleeps/reintentos, y bloquear uno de
los pocos workers de ese pool todo ese tiempo retrasaría el
procesamiento de otras cargas que compiten por el mismo pool mientras
esta solo espera. El procesador ya terminó y devolvió su resultado
antes de que esto corra — la confirmación llega después, solo por
Postgres (log_confirmacion_carga) y Teams, tanto si confirma (✅) como
si no (⚠️) — mismo canal donde ya caen los avisos de ftp_watcher.py.
"""

import threading
import time

from app.core.sqlserver import get_ruts_cargados
from app.core.postgres import registrar_confirmacion_carga, get_config_valor

_ESPERA_SEG_DEFAULT = 30.0
_REINTENTOS_DEFAULT = 3


def _config_numero(clave: str, default: float) -> float:
    valor = get_config_valor(clave)
    try:
        return float(valor) if valor else default
    except (TypeError, ValueError):
        return default


def confirmar_carga(
    caso: str,
    valores: list[str],
    columna: str = "TXTRUT",
    archivo_origen: str = "",
    usuario: str = "",
    progress_cb=None,
) -> dict:
    """
    Confirma que `valores` (el lote recién cargado) está en CONTACTOS
    para `caso` (mismo identificador que usa sqlserver.get_repetidos:
    SAV_AV, AV, PL, REFI, CARRITO, MKT), comparando por `columna`
    (TXTRUT por defecto; MKT usa TXTPATENTE).

    Tiempos de espera/reintento configurables desde config_global:
      - CONFIRMACION_ESPERA_SEG (default 30s entre cada intento)
      - CONFIRMACION_REINTENTOS (default 3 intentos)

    Retorna {caso, total_carga, total_confirmado, total_faltante,
    confirmado, faltantes} y no lanza excepción: get_ruts_cargados ya
    se traga los errores de conexión/consulta a SQL Server, y acá se
    envuelve además la resolución del caso (ej. IDDATABASE sin
    configurar) para que un caso mal configurado tampoco tumbe esta
    función a mitad de camino — solo se registra el error.
    """
    def emit(msg):
        if progress_cb:
            progress_cb(msg)

    espera_seg = _config_numero("CONFIRMACION_ESPERA_SEG", _ESPERA_SEG_DEFAULT)
    reintentos = int(_config_numero("CONFIRMACION_REINTENTOS", _REINTENTOS_DEFAULT))

    valores_limpios = {str(v).strip() for v in valores if str(v).strip()}
    total = len(valores_limpios)

    if total == 0:
        return {
            "caso": caso, "total_carga": 0, "total_confirmado": 0,
            "total_faltante": 0, "confirmado": True, "faltantes": [],
        }

    confirmados: set[str] = set()
    pendientes = set(valores_limpios)
    for intento in range(1, reintentos + 1):
        emit(f"Confirmando carga en Neotel — intento {intento}/{reintentos}")
        time.sleep(espera_seg)
        try:
            encontrados = get_ruts_cargados(caso, sorted(pendientes), columna=columna)
        except Exception as e:
            print(f"[confirmar_carga] Error consultando {caso}: {e}")
            encontrados = set()
        confirmados |= encontrados
        pendientes -= encontrados
        if not pendientes:
            break

    faltantes = sorted(pendientes)
    resultado = {
        "caso":              caso,
        "total_carga":       total,
        "total_confirmado":  len(confirmados),
        "total_faltante":    len(faltantes),
        "confirmado":        len(faltantes) == 0,
        "faltantes":         faltantes[:50],  # cap para no inflar log/notificación
    }

    try:
        registrar_confirmacion_carga(
            tipo_caso=caso,
            total_carga=total,
            total_confirmado=len(confirmados),
            confirmado=resultado["confirmado"],
            archivo_origen=archivo_origen,
            usuario=usuario,
        )
    except Exception as e:
        print(f"[confirmar_carga] No se pudo registrar en Postgres: {e}")

    if resultado["confirmado"]:
        emit(f"✅ Carga confirmada en Neotel: {len(confirmados)}/{total} registros")
        try:
            from app.core.ftp_watcher import _teams
            _teams(
                titulo=f"✅ Carga {caso} confirmada en BD Neotel",
                mensaje=(
                    f"Archivo: {archivo_origen or '—'}<br>"
                    f"Registros cargados: {total}<br>"
                    f"Ya aparecen todos en la BD de Neotel."
                ),
                color="28A745",
            )
        except Exception as e:
            print(f"[confirmar_carga] No se pudo notificar por Teams: {e}")
    else:
        emit(f"⚠️ Confirmación incompleta: {len(confirmados)}/{total} registros en BD Neotel")
        try:
            from app.core.ftp_watcher import _teams
            _teams(
                titulo=f"⚠️ Carga {caso} no confirmada en BD Neotel",
                mensaje=(
                    f"Archivo: {archivo_origen or '—'}<br>"
                    f"Registros subidos: {total}<br>"
                    f"Registros confirmados en BD: {len(confirmados)}<br>"
                    f"Registros faltantes: {len(faltantes)}"
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
) -> None:
    """
    Lanza confirmar_carga en un hilo daemon aparte y retorna de
    inmediato, sin esperar el resultado. Pensada para llamarse justo
    después de que un procesador (mkt.py, carrito_abandonado.py,
    refi_pl.py, sav_av.py) ya terminó y devolvió su resultado — la
    confirmación puede tardar hasta ~90s en sleeps/reintentos, y no
    tiene sentido que el caller (ni el worker del pool que lo procesó)
    se quede esperando eso.

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
        ),
        daemon=True,
        name=f"confirmar_carga-{caso}",
    )
    hilo.start()
