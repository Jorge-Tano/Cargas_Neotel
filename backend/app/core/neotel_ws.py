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
