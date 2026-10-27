"""
pendientes_carga.py
====================
Registros de SAV/AV/REFI/PL que llegaron ATRASADOS y esperan al próximo
horario regular para entrar a Neotel.

Por qué existe: las tareas de import de Leakage en Neotel (76/39/72/74)
solo importan el TXT `{prefijo}{AAAAMMDDHH}00.txt` de la hora ACTUAL de
Neotel (paso 4: GETDATE()), y si esa hora ya tiene lote cargado no
vuelven a importar (confirmado el 06-10-2026: los grupos 09:00/10:00
re-subidos como ...1300.txt a las 13:45 no generaron ningún lote). O sea,
un horario atrasado (backend caído, reintento) no tiene forma de entrar
por su cuenta: ni con su nombre original (la tarea no lo busca) ni con
el de la hora actual (o ya está usada, o se la "roba" al horario regular
que todavía no llega y lo bloquea).

Solución: el atrasado se guarda acá y se suma al TXT del siguiente
horario regular del mismo tipo — entra en un lote normal, sin tocar las
tareas de Neotel.
"""

from __future__ import annotations

import re
import threading
from pathlib import Path

import pandas as pd

_CARPETA = Path(__file__).resolve().parents[2] / "pendientes"
_lock = threading.Lock()

# "LEAKAGE_SAV_AM_0900_PARA_POBLAR_06102026_ENVIAR_2CALL.xlsx"
_RE_HORARIO = re.compile(r'(?<![A-Z0-9])(?:AM|PM)_(\d{3,4})(?![A-Z0-9])', re.IGNORECASE)
_RE_FECHA = re.compile(r'_(\d{2})(\d{2})(\d{4})_')


def sello_archivo(nombre_archivo: str) -> str | None:
    """AAAAMMDDHH del horario oficial del archivo, o None si el nombre no
    trae horario+fecha (ej. carga manual desde la UI)."""
    nombre = (nombre_archivo or "").upper()
    m_h = _RE_HORARIO.search(nombre)
    m_f = _RE_FECHA.search(nombre)
    if not m_h or not m_f:
        return None
    dd, mm, aaaa = m_f.groups()
    return f"{aaaa}{mm}{dd}{m_h.group(1).zfill(4)[:2]}"


def _ruta(tipo: str) -> Path:
    return _CARPETA / f"{tipo.upper()}.pkl"


def guardar(tipo: str, df: pd.DataFrame) -> int:
    """Agrega `df` a los pendientes de `tipo`. Retorna el total pendiente."""
    if df is None or len(df) == 0:
        return contar(tipo)
    with _lock:
        _CARPETA.mkdir(parents=True, exist_ok=True)
        ruta = _ruta(tipo)
        previo = pd.read_pickle(ruta) if ruta.exists() else None
        total = pd.concat([previo, df], ignore_index=True) if previo is not None else df.reset_index(drop=True)
        total.to_pickle(ruta)
        return len(total)


def tomar(tipo: str) -> pd.DataFrame:
    """Pendientes de `tipo` (sin borrarlos — ver limpiar)."""
    with _lock:
        ruta = _ruta(tipo)
        return pd.read_pickle(ruta) if ruta.exists() else pd.DataFrame()


def limpiar(tipo: str) -> None:
    with _lock:
        _ruta(tipo).unlink(missing_ok=True)


def contar(tipo: str) -> int:
    return len(tomar(tipo))
