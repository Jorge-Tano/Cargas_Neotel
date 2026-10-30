"""
notificaciones.py
==================
Avisos privados por Telegram. Cada aviso técnico del sistema tiene una
CATEGORÍA; las que se marcan en Configuración → Notificaciones (config_global
NOTIF_TELEGRAM_CATEGORIAS) se envían SOLO al bot de Telegram del responsable
(TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID en .env) y dejan de ir a Teams, donde
los ven todos los compañeros. El resumen "BDD Cargada Automáticamente" del
grupo Registros Leakage no pasa por acá: sigue siempre en Teams.

Si una categoría está marcada pero Telegram falla, el aviso NO se manda a
Teams (son avisos que se eligieron privados): queda en el log como error.
"""

from __future__ import annotations

import logging
import re
from html import unescape

import requests

try:
    # Igual que el bot de D:\Proyectos\Telegram_Bots: usa los certificados de
    # Windows, para la red de la empresa que inspecciona HTTPS con un
    # certificado propio (sin esto, api.telegram.org puede fallar por SSL).
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

from app.core.config import get_settings
from app.core.postgres import get_config_valor

logger = logging.getLogger(__name__)
settings = get_settings()

CLAVE_CONFIG = "NOTIF_TELEGRAM_CATEGORIAS"

# clave → (nombre en pantalla, qué incluye)
CATEGORIAS = {
    "errores": ("Errores y fallas",
                "Lote que no aparece en Neotel, cargas que fallan tras reintentos, cargas no confirmadas."),
    "detalle_cargas": ("Detalle de cada carga",
                       "Archivos nuevos detectados, procesando grupo y resultado técnico de cada horario."),
    "carga_mensual": ("Carga mensual PL/REFI",
                      "Avance y resultado del proceso del día 15: base, Resultante, cruce, aplicado o detenido."),
    "bases_regla": ("Regla de discado y bases",
                    "Cambio diario de la regla de SAV, bases nuevas detectadas y creación/asignación de las bases del mes."),
}


def telegram_configurado() -> bool:
    return bool(settings.telegram_bot_token.strip() and settings.telegram_chat_id.strip())


def categorias_privadas() -> set[str]:
    texto = get_config_valor(CLAVE_CONFIG) or ""
    return {c.strip() for c in texto.split(",") if c.strip() in CATEGORIAS}


def _a_texto_telegram(titulo: str, mensaje: str) -> str:
    """El HTML de los avisos de Teams (<br>, <b>, &nbsp;, tablas) a lo que
    acepta Telegram en parse_mode=HTML (solo <b>, <i>, <code>…)."""
    cuerpo = re.sub(r"<br\s*/?>|</tr>|</p>|</div>", "\n", mensaje, flags=re.IGNORECASE)
    cuerpo = re.sub(r"</t[dh]>", " · ", cuerpo, flags=re.IGNORECASE)
    cuerpo = re.sub(r"<(?!/?(b|i|code)\b)[^>]+>", "", cuerpo)     # deja solo <b>, <i>, <code>
    cuerpo = cuerpo.replace("&nbsp;", " ")
    cuerpo = unescape(cuerpo)
    cuerpo = cuerpo.replace("&", "&amp;").replace("<b>", "\x00b").replace("</b>", "\x00/b")
    cuerpo = cuerpo.replace("<i>", "\x00i").replace("</i>", "\x00/i").replace("<code>", "\x00c").replace("</code>", "\x00/c")
    cuerpo = cuerpo.replace("<", "&lt;").replace(">", "&gt;")
    cuerpo = (cuerpo.replace("\x00b", "<b>").replace("\x00/b", "</b>").replace("\x00i", "<i>")
              .replace("\x00/i", "</i>").replace("\x00c", "<code>").replace("\x00/c", "</code>"))
    cuerpo = re.sub(r"[ \t]+\n", "\n", cuerpo)
    cuerpo = re.sub(r"\n{3,}", "\n\n", cuerpo).strip()
    titulo_seguro = titulo.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"<b>{titulo_seguro}</b>\n{cuerpo}"[:4000]  # límite de Telegram: 4096


def enviar_telegram(titulo: str, mensaje: str) -> None:
    """Envía al bot del responsable. Lanza excepción si Telegram no lo acepta."""
    if not telegram_configurado():
        raise RuntimeError("Falta TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID en el .env del backend")
    r = requests.post(
        f"https://api.telegram.org/bot{settings.telegram_bot_token.strip()}/sendMessage",
        json={"chat_id": settings.telegram_chat_id.strip(), "text": _a_texto_telegram(titulo, mensaje),
              "parse_mode": "HTML", "disable_web_page_preview": True},
        timeout=15,
    )
    if not r.ok:
        # Si el formato HTML falla por algún caracter, se reintenta como texto plano.
        plano = re.sub(r"<[^>]+>", "", _a_texto_telegram(titulo, mensaje))
        r2 = requests.post(
            f"https://api.telegram.org/bot{settings.telegram_bot_token.strip()}/sendMessage",
            json={"chat_id": settings.telegram_chat_id.strip(), "text": unescape(plano)},
            timeout=15,
        )
        if not r2.ok:
            raise RuntimeError(f"Telegram respondió {r2.status_code}: {r2.text[:200]}")


def enviar_si_es_privado(categoria: str | None, titulo: str, mensaje: str) -> bool:
    """True si el aviso es de una categoría privada (se intentó por Telegram y
    NO debe ir a Teams). False → el llamador lo manda a Teams como siempre."""
    if not categoria or categoria not in categorias_privadas():
        return False
    try:
        enviar_telegram(titulo, mensaje)
    except Exception as e:
        logger.error("[Notificaciones] No se pudo enviar por Telegram (%s): %s — %s", categoria, titulo, e)
    return True
