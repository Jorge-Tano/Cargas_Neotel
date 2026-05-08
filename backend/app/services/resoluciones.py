"""
Procesamiento del TXT de resoluciones Neotel 17
"""

import io
import time
import unicodedata

import pandas as pd


# ─────────────────────────────────────────────
# Resoluciones pendientes
# ─────────────────────────────────────────────

RESOLUCIONES_PENDIENTES = {
    "lo pensará",
    "llamar más tarde",
    "llamar mas tarde",
    "cliente desea llamar más tarde",
    "cliente desea llamar mas tarde",
}


# ─────────────────────────────────────────────
# Normalizadores
# ─────────────────────────────────────────────

def _fix_mojibake(texto: str) -> str:
    """
    Corrige textos tipo:
      ResoluciÃ³n -> Resolución
    """

    try:
        return texto.encode("latin1").decode("utf-8")
    except Exception:
        return texto


def _normalizar_texto(texto: str) -> str:

    texto = str(texto)

    # arreglar encoding roto
    texto = _fix_mojibake(texto)

    texto = texto.strip().lower()

    texto = unicodedata.normalize("NFD", texto)

    texto = "".join(
        c for c in texto
        if unicodedata.category(c) != "Mn"
    )

    return texto


RESOLUCIONES_NORM = {
    _normalizar_texto(r)
    for r in RESOLUCIONES_PENDIENTES
}


# ─────────────────────────────────────────────
# Parsear TXT
# ─────────────────────────────────────────────

def leer_txt_resoluciones(contenido: bytes) -> pd.DataFrame:

    t0 = time.time()

    print(
        "[RESOLUCIONES] Decodificando TXT...",
        flush=True
    )

    texto = contenido.decode(
        "latin-1",
        errors="replace"
    )

    print(
        f"[RESOLUCIONES] TXT decodificado "
        f"({len(texto):,} chars)",
        flush=True
    )

    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Limpiando líneas...",
        flush=True
    )

    lineas_limpias = []

    for linea in texto.splitlines():

        l = linea.strip()

        if not l:
            continue

        if l.startswith("Gestos de sitio"):
            continue

        if l.startswith("/DOWNLOAD"):
            continue

        if l.startswith("/download"):
            continue

        lineas_limpias.append(linea)

    print(
        f"[RESOLUCIONES] Líneas útiles: "
        f"{len(lineas_limpias):,}",
        flush=True
    )

    texto_limpio = "\n".join(lineas_limpias)

    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Leyendo CSV con pandas...",
        flush=True
    )

    df = pd.read_csv(
        io.StringIO(texto_limpio),
        sep="|",
        dtype=str,
        on_bad_lines="skip",
        low_memory=False,
    )

    print(
        f"[RESOLUCIONES] DataFrame creado: "
        f"{len(df):,} filas",
        flush=True
    )

    # ─────────────────────────────────────────
    # Limpiar columnas
    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Limpiando columnas...",
        flush=True
    )

    columnas_nuevas = []

    for c in df.columns:

        c = str(c).strip()

        # arreglar encoding roto
        c = _fix_mojibake(c)

        columnas_nuevas.append(c)

    df.columns = columnas_nuevas

    # ─────────────────────────────────────────
    # Limpiar valores string
    # ─────────────────────────────────────────

    obj_cols = df.select_dtypes(include="object").columns

    df[obj_cols] = df[obj_cols].apply(
        lambda col: col.str.strip()
    )

    print(
        f"[RESOLUCIONES] TXT parseado en "
        f"{time.time() - t0:.2f}s",
        flush=True
    )

    return df


# ─────────────────────────────────────────────
# Detectar columnas
# ─────────────────────────────────────────────

def _detectar_columnas(df: pd.DataFrame) -> dict:

    print(
        "[RESOLUCIONES] Normalizando nombres columnas...",
        flush=True
    )

    columnas = {}

    for c in df.columns:

        c_str = str(c).strip()

        columnas[c_str] = c_str

        columnas[
            _normalizar_texto(c_str)
        ] = c_str

    print(
        f"[RESOLUCIONES] Total columnas detectadas: "
        f"{len(df.columns)}",
        flush=True
    )

    print(
        f"[RESOLUCIONES] Columnas disponibles: "
        f"{list(df.columns)}",
        flush=True
    )

    return columnas


# ─────────────────────────────────────────────
# Extraer pendientes
# ─────────────────────────────────────────────

def extraer_ruts_pendientes(
    df: pd.DataFrame
) -> set[str]:

    t0 = time.time()

    print(
        "[RESOLUCIONES] Iniciando extracción RUTs...",
        flush=True
    )

    columnas = _detectar_columnas(df)

    # ─────────────────────────────────────────
    # Buscar columna RUT
    # ─────────────────────────────────────────

    candidatos_rut = [
        "rut",
        "numrut",
        "num_rut",
    ]

    col_rut = None

    for candidato in candidatos_rut:

        candidato_norm = _normalizar_texto(candidato)

        if candidato_norm in columnas:
            col_rut = columnas[candidato_norm]
            break

    print(
        f"[RESOLUCIONES] Columna RUT encontrada: "
        f"{col_rut}",
        flush=True
    )

    # ─────────────────────────────────────────
    # Buscar columna resolución
    # ─────────────────────────────────────────

    candidatos_res = [
        "resolución",
        "resolucion",
        "resultado",
        "gestion",
        "gestión",
        "estado",
    ]

    col_res = None

    for candidato in candidatos_res:

        candidato_norm = _normalizar_texto(candidato)

        if candidato_norm in columnas:
            col_res = columnas[candidato_norm]
            break

    print(
        f"[RESOLUCIONES] Columna RES encontrada: "
        f"{col_res}",
        flush=True
    )

    # ─────────────────────────────────────────

    if not col_rut:
        raise ValueError(
            f"No se encontró columna RUT.\n"
            f"Columnas: {list(df.columns)}"
        )

    if not col_res:
        raise ValueError(
            f"No se encontró columna Resolución.\n"
            f"Columnas: {list(df.columns)}"
        )

    print(
        f"[RESOLUCIONES] Total filas TXT: "
        f"{len(df):,}",
        flush=True
    )

    # ─────────────────────────────────────────
    # Normalización vectorial rápida
    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Normalizando resoluciones...",
        flush=True
    )

    serie_res = (
        df[col_res]
        .fillna("")
        .astype(str)
        .apply(_normalizar_texto)
    )

    print(
        "[RESOLUCIONES] Construyendo mask...",
        flush=True
    )

    mask = serie_res.isin(
        RESOLUCIONES_NORM
    )

    total_match = int(mask.sum())

    print(
        f"[RESOLUCIONES] Coincidencias: "
        f"{total_match:,}",
        flush=True
    )

    # ─────────────────────────────────────────
    # Extraer RUTs
    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Extrayendo RUTs...",
        flush=True
    )

    ruts = (
        df.loc[mask, col_rut]
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    print(
        f"[RESOLUCIONES] Total RUTs: "
        f"{len(ruts):,}",
        flush=True
    )

    print(
        f"[RESOLUCIONES] Primeros RUTs: "
        f"{ruts[:20]}",
        flush=True
    )

    print(
        f"[RESOLUCIONES] Extracción completada "
        f"en {time.time() - t0:.2f}s",
        flush=True
    )

    return set(ruts)


# ─────────────────────────────────────────────
# Descargar TXT
# ─────────────────────────────────────────────

def descargar_txt_resoluciones(
    tipo: str
) -> tuple[bytes, str]:

    print(
        f"[RESOLUCIONES] Descargando TXT tipo={tipo}",
        flush=True
    )

    from app.core.ftp import descargar_txt_neotel17

    return descargar_txt_neotel17(tipo)


# ─────────────────────────────────────────────
# Proceso principal
# ─────────────────────────────────────────────

def procesar_resoluciones_pendientes(
    df_base: pd.DataFrame,
    col_rut_base: str,
    tipo: str,
    contenido_txt: bytes = None,
    nombre_txt: str = None,
) -> tuple[pd.DataFrame, int]:

    t0_total = time.time()

    print(
        "\n==============================",
        flush=True
    )

    print(
        "[RESOLUCIONES] INICIO PROCESO",
        flush=True
    )

    print(
        f"[RESOLUCIONES] Tipo: {tipo}",
        flush=True
    )

    print(
        f"[RESOLUCIONES] Base filas: "
        f"{len(df_base):,}",
        flush=True
    )

    # ─────────────────────────────────────────
    # Descargar TXT
    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Antes descargar TXT",
        flush=True
    )

    if contenido_txt is None:
        contenido_txt, nombre_txt = (
            descargar_txt_resoluciones(tipo)
        )

    print(
        f"[RESOLUCIONES] TXT descargado: "
        f"{nombre_txt}",
        flush=True
    )

    print(
        f"[RESOLUCIONES] Bytes TXT: "
        f"{len(contenido_txt):,}",
        flush=True
    )

    # ─────────────────────────────────────────
    # Parsear
    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Antes parsear TXT",
        flush=True
    )

    df_txt = leer_txt_resoluciones(
        contenido_txt
    )

    print(
        f"[RESOLUCIONES] TXT parseado OK: "
        f"{len(df_txt):,} filas",
        flush=True
    )

    # ─────────────────────────────────────────
    # Extraer pendientes
    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Antes extraer pendientes",
        flush=True
    )

    ruts_pendientes = (
        extraer_ruts_pendientes(df_txt)
    )

    print(
        f"[RESOLUCIONES] RUTs pendientes: "
        f"{len(ruts_pendientes):,}",
        flush=True
    )

    # ─────────────────────────────────────────
    # Cruce
    # ─────────────────────────────────────────

    print(
        "[RESOLUCIONES] Cruzando con base...",
        flush=True
    )

    ruts_base = (
        df_base[col_rut_base]
        .astype(str)
        .str.strip()
    )

    mask = ruts_base.isin(
        ruts_pendientes
    )

    df_resoluciones = (
        df_base[mask]
        .reset_index(drop=True)
    )

    print(
        f"[RESOLUCIONES] Resultado final: "
        f"{len(df_resoluciones):,}",
        flush=True
    )

    print(
        f"[RESOLUCIONES] Tiempo total: "
        f"{time.time() - t0_total:.2f}s",
        flush=True
    )

    print(
        "==============================\n",
        flush=True
    )

    return (
        df_resoluciones,
        len(df_resoluciones)
    )