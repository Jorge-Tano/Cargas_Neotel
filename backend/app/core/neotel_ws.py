"""
neotel_ws.py
=============
Dispara la tarea de import de una carga en Neotel al instante, vía su
WebService SOAP (ExecuteTask00), en vez de esperar a que corra el cron
interno de Neotel (que antes obligaba a esperar hasta el próximo xx:15
— ver confirmacion_carga.py). Confirmado con una prueba real: la llamada
responde en menos de 2 segundos, es decir, es síncrona — cuando el
WebService responde, el import ya insertó los registros en la BD de
Neotel, así que se puede confirmar casi de inmediato después.

Cada caso corresponde a una tarea distinta en Neotel (CALLCENTER >
Tareas), identificada por un IdTask numérico fijo (no cambia mes a mes,
a diferencia de IDDATABASE).
"""
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
from xml.sax.saxutils import escape

import requests

from app.core.config import get_settings

settings = get_settings()

_ID_TASK = {
    "SAV":      76,
    "AV":       39,
    "REFI":     72,
    "PL":       74,
    "CARRITO":  75,
    "MKT":      73,
    "PERDIDAS": 77,
    "AMALIA":   78,
    "OP_WHATSAPP": 79,
    "OP_PERDIDAS": 80,
}

# La Tarea única "Crear/Actualizar Base" (Id 83, ver app.services.campanas_mensuales)
# recibe sus parámetros directo por SOAP (ExecuteTaskNN) en vez de por
# Datasource+archivo — no usa este diccionario, ver ejecutar_tarea_con_parametros.

_NS = {"t": "http://tempuri.org/"}


def ejecutar_tarea(tipo: str, timeout: int = 30) -> str | None:
    """
    Dispara la tarea de carga de Neotel para `tipo` (SAV/AV/REFI/PL/
    CARRITO/MKT) y espera la respuesta. Retorna el mensaje que devuelve
    Neotel, o None si no se pudo disparar (tipo sin IdTask, credenciales
    sin configurar, o error de red/SOAP) — nunca lanza excepción: un
    fallo acá no debe tumbar el procesamiento del archivo, solo hace que
    confirmar_carga vuelva a su respaldo (esperar el próximo xx:15/16).
    """
    id_task = _ID_TASK.get(tipo)
    if id_task is None:
        print(f"[neotel_ws] Tipo '{tipo}' sin IdTask configurado, se omite.")
        return None

    if not settings.neotel_ws_user or not settings.neotel_ws_password:
        print("[neotel_ws] NEOTEL_WS_USER/NEOTEL_WS_PASSWORD no configurados, se omite.")
        return None

    soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header>
    <Authentication xmlns="http://tempuri.org/">
      <Username>{escape(settings.neotel_ws_user)}</Username>
      <Password>{escape(settings.neotel_ws_password)}</Password>
    </Authentication>
  </soap:Header>
  <soap:Body>
    <ExecuteTask00 xmlns="http://tempuri.org/">
      <idTask>{id_task}</idTask>
    </ExecuteTask00>
  </soap:Body>
</soap:Envelope>"""

    url = f"http://{settings.neotel_ws_host}/neoapi/webservice.asmx"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": '"http://tempuri.org/ExecuteTask00"',
    }

    try:
        resp = requests.post(url, data=soap_envelope.encode("utf-8"), headers=headers, timeout=timeout)
        if not resp.ok:
            # El cuerpo de una respuesta 500 suele traer el mensaje real del
            # error de .NET/SQL Server (raise_for_status() lo descarta) — se
            # imprime acá para poder diagnosticar sin adivinar.
            print(f"[neotel_ws] Error HTTP {resp.status_code} ejecutando tarea {tipo} (IdTask={id_task}). Respuesta:\n{resp.text[:2000]}")
            return None
        root = ET.fromstring(resp.content)
        nodo = root.find(".//t:ExecuteTask00Result", _NS)
        texto = nodo.text if nodo is not None else None
        print(f"[neotel_ws] Tarea {tipo} (IdTask={id_task}) ejecutada: {texto}")
        return texto or ""
    except Exception as e:
        print(f"[neotel_ws] Error ejecutando tarea {tipo} (IdTask={id_task}): {e}")
        return None


def subir_y_disparar_carga_horaria(
    df_carga, tipo: str, carpeta: str, prefijo: str, columnas, alias=None,
    nombre_archivo: str = "", col_rut: str = "RUT", ruts_repetidos: set | None = None,
) -> dict:
    """
    Genera el TXT de carga de SAV/AV/REFI/PL, lo sube a Neotel17 y dispara
    su tarea de import.

    Las tareas de Leakage (76/39/72/74) importan solo
    `{prefijo}{AAAAMMDDHH}00.txt` de la hora ACTUAL de Neotel, y no
    vuelven a importar si esa hora ya tiene lote (ver
    app.services.pendientes_carga). Por eso:
      - Si el archivo de origen es de una hora ANTERIOR a la actual
        (atrasado), no se sube: sus registros se guardan como pendientes.
      - Si está a tiempo, se le suman los pendientes de ese tipo (sin los
        RUT que ya están en la base ni duplicados) y se sube todo junto
        con el nombre de la hora actual. Si el import se dispara bien, los
        pendientes se limpian.

    Retorna {path_txt, carga_forzada, hora_disparo, atrasado,
    pendientes_incluidos, df_subido}.
    """
    from app.core.sqlserver import obtener_hora_neotel
    from app.core.ftp_neotel17 import subir_archivo_carga_txt
    from app.services.utils import exportar_txt_carga
    from app.services import pendientes_carga

    resultado = {
        "path_txt": None, "carga_forzada": False, "hora_disparo": None,
        "atrasado": False, "pendientes_incluidos": 0, "df_subido": df_carga,
    }

    hora = obtener_hora_neotel() or datetime.now()
    sello_origen = pendientes_carga.sello_archivo(nombre_archivo)
    if sello_origen and sello_origen < hora.strftime("%Y%m%d%H"):
        total = pendientes_carga.guardar(tipo, df_carga)
        print(f"[neotel_ws] {tipo}: {nombre_archivo} llegó atrasado ({sello_origen}), "
              f"{len(df_carga)} registro(s) quedan pendientes para el próximo horario ({total} en total)")
        resultado["atrasado"] = True
        return resultado

    pendientes = pendientes_carga.tomar(tipo)
    if len(pendientes) > 0:
        if col_rut in pendientes.columns and ruts_repetidos:
            pendientes = pendientes[~pendientes[col_rut].astype(str).str.strip().isin(ruts_repetidos)]
        combinado = pd.concat([df_carga, pendientes], ignore_index=True)
        if col_rut in combinado.columns:
            combinado = combinado.drop_duplicates(subset=[col_rut], keep="first")
        resultado["pendientes_incluidos"] = len(combinado) - len(df_carga)
        df_carga = combinado.reset_index(drop=True)
        resultado["df_subido"] = df_carga

    for _ in range(2):
        sello = hora.strftime("%Y%m%d%H") + "00"
        path_txt = exportar_txt_carga(df_carga, f"{carpeta}/{prefijo}{sello}.txt", columnas, alias=alias)
        resultado["path_txt"] = path_txt
        if not path_txt:
            break
        try:
            subir_archivo_carga_txt(path_txt, tipo=tipo)
        except Exception as e:
            print(f"[WARN] Error subiendo TXT de {tipo} por FTP a Neotel17: {e}")
            break

        hora_disparo = obtener_hora_neotel()
        if hora_disparo and hora_disparo.strftime("%Y%m%d%H") != hora.strftime("%Y%m%d%H"):
            # Cambió la hora entre nombrar y disparar: la tarea buscaría el
            # TXT de la hora nueva. Se renombra y se vuelve a subir.
            print(f"[neotel_ws] {tipo}: cambió la hora antes de disparar ({sello}), se renombra el TXT")
            hora = hora_disparo
            continue
        resultado["hora_disparo"] = hora_disparo
        resultado["carga_forzada"] = ejecutar_tarea(tipo) is not None
        break

    if resultado["carga_forzada"] and len(pendientes) > 0:
        pendientes_carga.limpiar(tipo)
    return resultado


def base_de_campania(campania: int) -> str | None:
    """SAL_BASES actual de la campaña de salida en Callcenter/Telefonía."""
    from app.core.sqlserver import sqlserver_cursor
    with sqlserver_cursor("master") as cursor:
        cursor.execute(
            f"SELECT SAL_BASES FROM [{settings.sqlserver_linked_host}].[TEL_CALLCENTER].[dbo].[_CAMPAÑAS] WHERE [CAMPAÑA] = ?",
            [campania],
        )
        fila = cursor.fetchone()
    return (fila[0] or "").strip() if fila else None


def asignar_base_a_campania(campania: int, iddatabase: int, cliente: str = "1000", timeout: int = 60) -> str:
    """
    Asigna `iddatabase` como base de salida (SAL_BASES) de la campaña, con el
    método oficial Update_Campaign_Object del WebService (recibe solo las
    columnas a cambiar de _CAMPAÑAS; probado el 30-10-2026 en la campaña 218:
    no toca ninguna otra columna). La campaña admite una sola base.
    Confirma leyendo _CAMPAÑAS; si no quedó, lanza RuntimeError.
    """
    import json
    payload = {"CLIENTE": cliente, "CAMPAÑA": int(campania), "SAL_BASES": str(int(iddatabase))}
    soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header>
    <Authentication xmlns="http://tempuri.org/">
      <Username>{escape(settings.neotel_ws_user)}</Username>
      <Password>{escape(settings.neotel_ws_password)}</Password>
    </Authentication>
  </soap:Header>
  <soap:Body>
    <Update_Campaign_Object xmlns="http://tempuri.org/">
      <campaignJson>{escape(json.dumps(payload, ensure_ascii=False))}</campaignJson>
    </Update_Campaign_Object>
  </soap:Body>
</soap:Envelope>"""
    resp = requests.post(
        f"http://{settings.neotel_ws_host}/neoapi/webservice.asmx",
        data=soap_envelope.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": '"http://tempuri.org/Update_Campaign_Object"'},
        timeout=timeout,
    )
    texto = ""
    try:
        nodo = ET.fromstring(resp.content).find(".//t:Update_Campaign_ObjectResult", _NS)
        texto = (nodo.text or "") if nodo is not None else resp.text[:300]
    except Exception:
        texto = resp.text[:300]
    actual = base_de_campania(campania)
    if actual != str(int(iddatabase)):
        raise RuntimeError(f"La campaña {campania} sigue con la base {actual!r} (HTTP {resp.status_code}: {texto})")
    print(f"[neotel_ws] Campaña {campania} → base {iddatabase}: {texto}")
    return texto


def ejecutar_tarea_con_parametros(id_task: int, parametros: list[str], timeout: int = 30) -> str | None:
    """
    Variante de `ejecutar_tarea` para tareas que reciben parámetros
    directo por SOAP (ExecuteTaskNN, NN = cantidad de parámetros — ver
    WSDL: existen ExecuteTask00 a ExecuteTask22, todos con la misma forma
    `(idTask, param1..paramNN)`, todos string). Se usa para la Tarea
    única "Crear/Actualizar Base" (ClienteCod + TipoOperacion + el resto
    de los campos de DB_INSERT/DB_UPDATE) en vez de subir un archivo por
    FTP — evita la espera/polling y el riesgo de que dos ECRM (PL/REFI)
    se pisen el archivo el mismo día.

    Retorna el mensaje que devuelve Neotel, o None si no se pudo
    disparar (igual que `ejecutar_tarea`, nunca lanza excepción).
    """
    n = len(parametros)
    if not (0 <= n <= 22):
        print(f"[neotel_ws] Cantidad de parámetros inválida ({n}), debe ser 0-22.")
        return None

    if not settings.neotel_ws_user or not settings.neotel_ws_password:
        print("[neotel_ws] NEOTEL_WS_USER/NEOTEL_WS_PASSWORD no configurados, se omite.")
        return None

    metodo = f"ExecuteTask{n:02d}"
    params_xml = "\n".join(
        f"      <param{i + 1}>{escape(str(valor))}</param{i + 1}>"
        for i, valor in enumerate(parametros)
    )
    soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header>
    <Authentication xmlns="http://tempuri.org/">
      <Username>{escape(settings.neotel_ws_user)}</Username>
      <Password>{escape(settings.neotel_ws_password)}</Password>
    </Authentication>
  </soap:Header>
  <soap:Body>
    <{metodo} xmlns="http://tempuri.org/">
      <idTask>{id_task}</idTask>
{params_xml}
    </{metodo}>
  </soap:Body>
</soap:Envelope>"""

    url = f"http://{settings.neotel_ws_host}/neoapi/webservice.asmx"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": f'"http://tempuri.org/{metodo}"',
    }

    try:
        resp = requests.post(url, data=soap_envelope.encode("utf-8"), headers=headers, timeout=timeout)
        if not resp.ok:
            print(f"[neotel_ws] Error HTTP {resp.status_code} ejecutando {metodo} (IdTask={id_task}). Respuesta:\n{resp.text[:2000]}")
            return None
        root = ET.fromstring(resp.content)
        nodo = root.find(f".//t:{metodo}Result", _NS)
        texto = nodo.text if nodo is not None else None
        print(f"[neotel_ws] {metodo} (IdTask={id_task}) ejecutada: {texto}")
        return texto or ""
    except Exception as e:
        print(f"[neotel_ws] Error ejecutando {metodo} (IdTask={id_task}): {e}")
        return None
