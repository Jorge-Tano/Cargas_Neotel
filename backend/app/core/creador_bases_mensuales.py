"""
creador_bases_mensuales.py
===========================
Crea solas las bases mensuales del MES SIGUIENTE (ej. "NEO/SAV LEAKAGE
NOVIEMBRE 2026") el último día del mes a las 23:50 (hora de Neotel), si
todavía no existen. Antes se creaban a mano el día 1 (y con errores de
nombre: la de AV de octubre quedó "NEO/SAV LEAKAGE..." y Neotel siguió
cargando en la de septiembre).

Por qué a esa hora y no antes: las Tareas de carga de Neotel usan "la base
más nueva cuyo nombre empieza con …" (ej. MAX(IDDATABASE) WHERE DESCRIPCION
LIKE 'NEO/AV LEAKAGE%'). Si la de noviembre existiera antes de terminar
octubre, las cargas de los últimos días de octubre caerían en ella. A las
23:50 del último día ya no hay cargas. Si el backend estuvo caído a esa
hora, las crea apenas vuelva, siempre antes de las 08:00 del día 1.

Cada base nueva copia la configuración (intentos, orden de discado,
teléfonos, etc.) de la base del mes que termina del mismo tipo, y se crea
con la Tarea 83 (campanas_mensuales.crear_base). Después se asigna como base
de salida de su campaña en Callcenter/Telefonía (_CAMPAÑAS.SAL_BASES, una
sola base por campaña) con el método oficial Update_Campaign_Object del
WebService (neotel_ws.asignar_base_a_campania) — también si la base ya
existía pero la campaña seguía en la del mes anterior. La regla de discado
de SAV la pone app.core.regla_discado con el primer lote del día.

Tipos habilitados en config_global CREAR_BASES_MENSUALES (separados por
coma). Sin configurar: SAV,PL,REFI — los que la Tarea 83 ya sabe crear
(0265/0001/0289-CREAR). AV, AMALIA, OP_PERDIDAS y OP_WHATSAPP necesitan
antes su Datasource de crear base y su condición en la Tarea 83 (0250,
0059, 0290); sin eso la Tarea responde "Combinación no reconocida".
"""

from __future__ import annotations

import asyncio
import calendar
import logging
from datetime import date, datetime

from app.core.postgres import get_config_valor

logger = logging.getLogger(__name__)

_INTERVALO_SEG = 5 * 60
_HORA_CREACION = (23, 50)      # último día del mes, hora de Neotel
_HORA_LIMITE_DIA_1 = 8         # si se perdió la ventana, recuperar el día 1 antes de las 08:00
_TIPOS_DEFAULT = "SAV,PL,REFI"

_MESES = ["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO",
          "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"]

# tipo → (nombre de la base para (año, mes), prefijo común para encontrar la del mes anterior)
_BASES = {
    "SAV":         (lambda a, m: f"NEO/SAV LEAKAGE {_MESES[m - 1]} {a}",      "NEO/SAV LEAKAGE "),
    "AV":          (lambda a, m: f"NEO/AV LEAKAGE {_MESES[m - 1]} {a}",       "NEO/AV LEAKAGE "),
    "PL":          (lambda a, m: f"BASE PL LEAKAGE {_MESES[m - 1]} {a}",      "BASE PL LEAKAGE "),
    "REFI":        (lambda a, m: f"BASE RN LEAKAGE {_MESES[m - 1]} {a}",      "BASE RN LEAKAGE "),
    "AMALIA":      (lambda a, m: f"BDD AMALIA {_MESES[m - 1][:3]} {a}",       "BDD AMALIA "),
    "OP_PERDIDAS": (lambda a, m: f"Perdidas Amalia {_MESES[m - 1].capitalize()} {a}", "Perdidas Amalia "),
    "OP_WHATSAPP": (lambda a, m: f"WHTSP {_MESES[m - 1]} {a}",                "WHTSP "),
}

# tipo → campaña de salida en Callcenter/Telefonía (_CAMPAÑAS.SAL_BASES, admite
# una sola base). Se puede cambiar con config_global CAMPANIA_{TIPO}.
# OP_PERDIDAS no tiene campaña propia: la 241 OPCIONES DE PAGO usa la de WhatsApp.
_CAMPANIAS = {"SAV": 234, "AV": 218, "PL": 38, "REFI": 39, "AMALIA": 7, "OP_WHATSAPP": 241}

_hecho: set[str] = set()           # "AAAA-MM" ya completado en esta ejecución del backend
_avisado_error: set[str] = set()   # "AAAA-MM" con aviso de error ya enviado


def _mes_siguiente(d: date) -> tuple[int, int]:
    return (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)


def tipos_habilitados() -> list[str]:
    texto = (get_config_valor("CREAR_BASES_MENSUALES") or "").strip() or _TIPOS_DEFAULT
    return [t.strip().upper() for t in texto.split(",") if t.strip().upper() in _BASES]


def _buscar_bases(tipo: str, prefijo: str) -> list[tuple[int, str]]:
    from app.core.config import get_settings
    from app.core.sqlserver import sqlserver_cursor
    from app.services.campanas_mensuales import _db
    linked = get_settings().sqlserver_linked_host
    with sqlserver_cursor("master") as cursor:
        cursor.execute(f"""
            SELECT IDDATABASE, DESCRIPCION FROM [{linked}].[{_db(tipo)}].[dbo].[DB]
            WHERE DESCRIPCION LIKE ? AND ISNULL(ESTADO, '') <> '6'
            ORDER BY IDDATABASE DESC
        """, [prefijo + "%"])
        return [(int(r[0]), (r[1] or "").strip()) for r in cursor.fetchall()]


def plan(anio: int, mes: int) -> list[dict]:
    """Qué base corresponde a cada tipo habilitado para (año, mes), si ya
    existe y de qué base se copiaría la configuración. Solo lectura."""
    filas = []
    for tipo in tipos_habilitados():
        nombre_fn, prefijo = _BASES[tipo]
        nombre = nombre_fn(anio, mes)
        try:
            bases = _buscar_bases(tipo, prefijo)
            existente = next((b for b in bases if b[1].upper() == nombre.upper()), None)
            modelo = next((b for b in bases if b[1].upper() != nombre.upper()), None)
            filas.append({"tipo": tipo, "nombre": nombre,
                          "existe": existente[0] if existente else None,
                          "modelo": {"iddatabase": modelo[0], "descripcion": modelo[1]} if modelo else None,
                          "error": None})
        except Exception as e:
            filas.append({"tipo": tipo, "nombre": nombre, "existe": None, "modelo": None, "error": str(e)})
    return filas


def _campania(tipo: str) -> int | None:
    valor = (get_config_valor(f"CAMPANIA_{tipo}") or "").strip()
    if valor.isdigit():
        return int(valor)
    return _CAMPANIAS.get(tipo)


def _asignar_campania(tipo: str, iddatabase: int) -> tuple[bool, str, bool]:
    """(ok, texto, cambió algo). Deja la base nueva como base de salida de la
    campaña del tipo (si tiene); si ya la tenía, no hace nada."""
    from app.core.neotel_ws import base_de_campania, asignar_base_a_campania
    campania = _campania(tipo)
    if not campania:
        return True, "sin campaña propia", False
    try:
        actual = base_de_campania(campania)
        if actual == str(iddatabase):
            return True, f"campaña {campania} ya la tenía", False
        asignar_base_a_campania(campania, iddatabase)
        logger.info("[CreadorBases] %s: campaña %s pasó de la base %s a la %s", tipo, campania, actual, iddatabase)
        return True, f"campaña {campania}: {actual} → {iddatabase}", True
    except Exception as e:
        logger.exception("[CreadorBases] %s: no se pudo asignar la base %s a la campaña %s", tipo, iddatabase, campania)
        return False, f"ERROR asignando a la campaña {campania}: {e}", False


def crear_faltantes(anio: int, mes: int) -> list[dict]:
    """Crea las bases de (año, mes) que falten y las asigna a su campaña de
    Callcenter/Telefonía. No lanza excepción."""
    from app.services.campanas_mensuales import obtener_base, crear_base

    resultados = []
    for p in plan(anio, mes):
        fila = {**p, "ok": False, "cambio": False, "campania": ""}
        iddatabase = None
        if p["error"]:
            fila["resultado"] = f"error leyendo bases: {p['error']}"
        elif p["existe"]:
            iddatabase = p["existe"]
            fila["resultado"] = f"ya existía (Nº {iddatabase})"
        elif not p["modelo"]:
            fila["resultado"] = "sin base anterior de la cual copiar la configuración"
        else:
            try:
                valores = obtener_base(p["tipo"], p["modelo"]["iddatabase"])
                valores = {**valores, "descripcion": p["nombre"]}
                if str(valores.get("estado")) in ("4", "5"):  # la del mes que termina ya se cerró: la nueva parte activa
                    valores["estado"] = "1"
                r = crear_base(p["tipo"], valores, usuario="sistema", omitir_verificacion=True)
                iddatabase = r["iddatabase"]
                fila["resultado"] = f"creada (Nº {iddatabase})"
                fila["cambio"] = True
                logger.info("[CreadorBases] %s: creada %s (Nº %s), configuración copiada de Nº %s",
                            p["tipo"], p["nombre"], iddatabase, p["modelo"]["iddatabase"])
            except Exception as e:
                logger.exception("[CreadorBases] %s: no se pudo crear %s", p["tipo"], p["nombre"])
                fila["resultado"] = f"ERROR: {e}"

        if iddatabase:
            ok_camp, texto_camp, cambio_camp = _asignar_campania(p["tipo"], iddatabase)
            fila["campania"] = texto_camp
            fila["ok"] = ok_camp
            fila["cambio"] = fila["cambio"] or cambio_camp
        resultados.append(fila)
    return resultados


def _avisar(resultados: list[dict], anio: int, mes: int) -> None:
    ok = all(r["ok"] for r in resultados)
    lineas = "<br>".join(
        f"<b>{r['tipo']}</b> · {r['nombre']}: {r['resultado']}" + (f" · {r['campania']}" if r["campania"] else "")
        for r in resultados
    )
    try:
        from app.core.ftp_watcher import _teams
        _teams(
            titulo=f"{'✅' if ok else '⚠️'} Bases de {_MESES[mes - 1].capitalize()} {anio}",
            mensaje=lineas + ("" if ok else "<br><br>Lo que falló hay que hacerlo a mano (crear la base o asignarla a la campaña)."),
            color="28A745" if ok else "FFA500",
            categoria="bases_regla",
        )
    except Exception as e:
        logger.warning("[CreadorBases] No se pudo notificar por Teams: %s", e)


def _toca_ahora(ahora: datetime) -> tuple[int, int] | None:
    """(año, mes) a crear si estamos en la ventana: último día ≥ 23:50, o
    día 1 antes de las 08:00 (recuperación si el backend estuvo caído)."""
    ultimo_dia = calendar.monthrange(ahora.year, ahora.month)[1]
    if ahora.day == ultimo_dia and (ahora.hour, ahora.minute) >= _HORA_CREACION:
        return _mes_siguiente(ahora.date())
    if ahora.day == 1 and ahora.hour < _HORA_LIMITE_DIA_1:
        return ahora.year, ahora.month
    return None


def verificar() -> None:
    from app.core.sqlserver import obtener_hora_neotel
    ahora = obtener_hora_neotel() or datetime.now()
    objetivo = _toca_ahora(ahora)
    if not objetivo:
        return
    clave = f"{objetivo[0]:04d}-{objetivo[1]:02d}"
    if clave in _hecho:
        return
    resultados = crear_faltantes(*objetivo)
    completo = all(r["ok"] for r in resultados)
    creo_algo = any(r["cambio"] for r in resultados)
    # Avisar si creó algo, o la primera vez que algo falla (los reintentos
    # cada 5 min hasta las 08:00 no repiten el aviso de error).
    if creo_algo or (not completo and clave not in _avisado_error):
        _avisar(resultados, *objetivo)
    if completo:
        _hecho.add(clave)  # no volver a revisar este mes
    else:
        _avisado_error.add(clave)  # se reintenta en la próxima pasada mientras dure la ventana


async def _loop():
    logger.info("[CreadorBases] Loop iniciado.")
    while True:
        try:
            await asyncio.get_event_loop().run_in_executor(None, verificar)
        except Exception:
            logger.exception("[CreadorBases] Error en verificar")
        await asyncio.sleep(_INTERVALO_SEG)


def arrancar_creador_bases_mensuales():
    asyncio.ensure_future(_loop())
