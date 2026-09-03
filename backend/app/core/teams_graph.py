"""
teams_graph.py
===============
Publica mensajes directo en el grupo de chat de Teams "Registros
Leakage" (Supervisores/Jefes) vía Microsoft Graph API, sin pasar por
Power Automate.

Por qué no un webhook simple: Microsoft Graph no ofrece ChatMessage.Send
como permiso de Aplicación (solo Delegado) — no hay forma de que una app
"pura de servicio" mande mensajes a un chat cualquiera sin una persona
real detrás autorizando. Así que esto se autentica como un usuario real
(jorge.gomez@2callcenter.com, ya miembro del chat) vía "device code
flow": una vez a mano (ver `iniciar_sesion()`), y después el token se
refresca solo indefinidamente sin volver a pedir login.

Implementado con OAuth2 puro (requests), sin la librería `msal`: en este
entorno, el cliente HTTP interno de MSAL corta la conexión reutilizada
(keep-alive) al sondear repetidamente el endpoint de token durante el
device code flow ("RemoteDisconnected") — una petición suelta al mismo
endpoint funciona bien, así que se implementa el protocolo directo.

Requiere en .env:
    GRAPH_CLIENT_ID     — Application (client) ID de la app en Entra ID
    GRAPH_TENANT_ID     — Directory (tenant) ID
    GRAPH_CHAT_NOMBRE   — nombre del chat de grupo destino (default: "Registros Leakage")

Y en la app de Entra ID:
    - "Allow public client flows" = Yes (Authentication → Configuración)
    - Permisos delegados: ChatMessage.Send, Chat.Read (con consentimiento)

Primer uso (una sola vez, interactivo):
    python -m app.core.teams_graph login
"""

from __future__ import annotations

import json
import os
import time

import requests

from app.core.config import get_settings

settings = get_settings()

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"
_SCOPE = "ChatMessage.Send Chat.Read offline_access"
_CACHE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".token_cache_teams.json")

# Margen antes de que expire el access_token para renovarlo por
# adelantado (evita usar uno que vence a mitad de una llamada).
_MARGEN_EXPIRACION_SEG = 120

# Cache en memoria del ID del chat "Registros Leakage" (no cambia entre
# corridas) — evita resolverlo por nombre en cada mensaje.
_chat_id_cache: str | None = None


def _authority() -> str:
    if not settings.graph_client_id or not settings.graph_tenant_id:
        raise ValueError(
            "GRAPH_CLIENT_ID / GRAPH_TENANT_ID no configurados en .env "
            "(app de Entra ID para notificaciones a Teams)."
        )
    return f"https://login.microsoftonline.com/{settings.graph_tenant_id}"


def _cargar_cache() -> dict:
    if os.path.exists(_CACHE_PATH):
        with open(_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _guardar_cache(datos: dict) -> None:
    with open(_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(datos, f)


def iniciar_sesion() -> None:
    """
    Login interactivo (device code flow, OAuth2 puro) — correr UNA sola
    vez a mano:
        python -m app.core.teams_graph login
    Muestra una URL + código; hay que abrir esa URL en un navegador,
    iniciar sesión con la cuenta que es miembro del chat destino, y
    listo — el token (con refresh_token) queda guardado en disco para
    que el backend lo use solo de ahí en adelante.
    """
    authority = _authority()

    r = requests.post(
        f"{authority}/oauth2/v2.0/devicecode",
        data={"client_id": settings.graph_client_id, "scope": _SCOPE},
        timeout=15,
    )
    r.raise_for_status()
    flow = r.json()
    print(flow["message"])  # incluye la URL y el código a ingresar

    device_code = flow["device_code"]
    intervalo = flow.get("interval", 5)
    expira_en = flow.get("expires_in", 900)

    inicio = time.time()
    while time.time() - inicio < expira_en:
        time.sleep(intervalo)
        try:
            r = requests.post(
                f"{authority}/oauth2/v2.0/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "client_id": settings.graph_client_id,
                    "device_code": device_code,
                },
                timeout=15,
            )
        except requests.RequestException as e:
            print(f"[teams_graph] hipo de red sondeando, reintentando: {e}")
            continue

        body = r.json()
        if r.status_code == 200:
            body["_obtenido_en"] = time.time()
            _guardar_cache(body)
            print("Login OK — token guardado. El backend ya puede publicar en Teams sin pedir login de nuevo.")
            return

        error = body.get("error")
        if error == "authorization_pending":
            continue
        if error == "slow_down":
            intervalo += 5
            continue
        raise RuntimeError(f"Login fallido: {body.get('error_description', body)}")

    raise RuntimeError("El código expiró antes de completar el login. Correr de nuevo: python -m app.core.teams_graph login")


def _refrescar_token(cache: dict) -> dict:
    r = requests.post(
        f"{_authority()}/oauth2/v2.0/token",
        data={
            "grant_type": "refresh_token",
            "client_id": settings.graph_client_id,
            "refresh_token": cache["refresh_token"],
            "scope": _SCOPE,
        },
        timeout=15,
    )
    r.raise_for_status()
    nuevo = r.json()
    nuevo["_obtenido_en"] = time.time()
    _guardar_cache(nuevo)
    return nuevo


def _obtener_token() -> str:
    """
    Token válido para llamar a Graph, renovado en silencio con el
    refresh_token guardado si hace falta (sin volver a pedir login).
    Lanza RuntimeError si nunca se hizo el login inicial (`iniciar_sesion()`)
    o si el refresh token quedó inválido (hay que volver a loguearse a mano).
    """
    cache = _cargar_cache()
    if not cache.get("refresh_token"):
        raise RuntimeError(
            "No hay sesión guardada para Teams/Graph. Correr una vez: "
            "python -m app.core.teams_graph login"
        )

    vencido = time.time() >= cache.get("_obtenido_en", 0) + cache.get("expires_in", 0) - _MARGEN_EXPIRACION_SEG
    if vencido:
        try:
            cache = _refrescar_token(cache)
        except Exception as e:
            raise RuntimeError(
                f"No se pudo renovar el token de Teams/Graph ({e}) — hay que volver a "
                f"correr: python -m app.core.teams_graph login"
            )

    return cache["access_token"]


def _resolver_chat_id() -> str:
    global _chat_id_cache
    if _chat_id_cache:
        return _chat_id_cache

    token = _obtener_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{_GRAPH_BASE}/me/chats?$filter=chatType eq 'group'"

    nombre_buscado = settings.graph_chat_nombre.strip().lower()
    while url:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        for chat in data.get("value", []):
            if (chat.get("topic") or "").strip().lower() == nombre_buscado:
                _chat_id_cache = chat["id"]
                return _chat_id_cache
        url = data.get("@odata.nextLink")

    raise RuntimeError(
        f"No se encontró un chat de grupo llamado '{settings.graph_chat_nombre}' "
        f"entre los chats de la cuenta logueada."
    )


def enviar_mensaje_chat(titulo: str, mensaje_html: str) -> None:
    """
    Publica un mensaje en el chat configurado (GRAPH_CHAT_NOMBRE). No
    lanza excepción — cualquier error queda solo impreso, para no
    tumbar el flujo que llama a esto (igual que _teams() en ftp_watcher.py).
    """
    try:
        chat_id = _resolver_chat_id()
        token = _obtener_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "body": {
                "contentType": "html",
                "content": f"<h3>{titulo}</h3>{mensaje_html}",
            }
        }
        r = requests.post(f"{_GRAPH_BASE}/chats/{chat_id}/messages", headers=headers, json=body, timeout=15)
        r.raise_for_status()
        print(f"[teams_graph] Mensaje publicado en '{settings.graph_chat_nombre}': {titulo}")
    except Exception as e:
        print(f"[teams_graph] Error publicando en Teams: {e}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "login":
        iniciar_sesion()
    else:
        print("Uso: python -m app.core.teams_graph login")
