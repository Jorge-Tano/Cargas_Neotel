import io
import pandas as pd
import re

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

# ─────────────────────────────────────────────
# FORMATEO DE TELÉFONOS
# ─────────────────────────────────────────────


def leer_archivo(archivo_bytes: bytes, nombre_archivo: str) -> pd.DataFrame:
    """
    Lee cualquier formato de archivo detectando el formato REAL por contenido,
    no por extension. Soporta xls, xlsx, xlsm, csv.
    """
    nombre = nombre_archivo.lower()

    # CSV por extension (no tiene header binario)
    if nombre.endswith(".csv"):
        return pd.read_csv(
            io.BytesIO(archivo_bytes),
            sep=None,
            engine="python",
            encoding="latin1",
            dtype=str
        )

    # Detectar formato real por los primeros bytes (magic bytes)
    header = archivo_bytes[:4]
    es_zip = header[:2] == b"PK"          # xlsx/xlsm son ZIP
    es_ole = header == b"\xd0\xcf\x11\xe0"  # xls antiguo es OLE2

    if es_zip:
        # Es xlsx o xlsm real
        return pd.read_excel(io.BytesIO(archivo_bytes), engine="openpyxl", dtype=str)
    elif es_ole:
        # Es xls antiguo real
        return pd.read_excel(io.BytesIO(archivo_bytes), engine="xlrd", dtype=str)
    else:
        # Intentar openpyxl primero, luego xlrd como fallback
        try:
            return pd.read_excel(io.BytesIO(archivo_bytes), engine="openpyxl", dtype=str)
        except Exception:
            try:
                return pd.read_excel(io.BytesIO(archivo_bytes), engine="xlrd", dtype=str)
            except Exception as e:
                raise ValueError(f"No se pudo leer el archivo {nombre_archivo}: {e}")


def agregar_cero(numero) -> str:
    """
    Equivale a: =SI(IZQUIERDA(P2;1)="2";P2;"0"&P2)
    y también a: =CONCATENAR("0";G2)

    - Si el número ya empieza con "0" → lo deja igual
    - Si empieza con "2" (teléfono fijo) → lo deja igual
    - Si no → agrega "0" adelante
    - Si es 0, nulo o vacío → retorna "00"
    """
    num = str(numero).strip().replace(".0", "")

    if not num or num in ("0", "nan", "None", ""):
        return "00"

    # Eliminar caracteres no numéricos
    num = re.sub(r"\D", "", num)

    if not num:
        return "00"

    if num.startswith("0") or num.startswith("2"):
        return num

    return "0" + num


def formatear_columnas_telefono(df: pd.DataFrame, columnas: list[str]) -> pd.DataFrame:
    """
    Aplica agregar_cero a todas las columnas de teléfono indicadas.
    """
    for col in columnas:
        if col in df.columns:
            df[col] = df[col].apply(agregar_cero)
    return df


def leer_resolucion_txt(txt_bytes: bytes) -> pd.DataFrame:
    """
    Lee un TXT de resoluciones Neotel (delimitado por '|', encoding latin1),
    como los de /DOWNLOAD/Resultante_PL o Resultante_SAV en el FTP Neotel 17.
    """
    df = pd.read_csv(
        io.BytesIO(txt_bytes),
        sep="|",
        dtype=str,
        encoding="latin1",
    )
    # La última columna suele quedar vacía (el archivo termina en '|')
    df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
    df.columns = df.columns.str.strip()
    return df


def formatear_porcentaje(valor) -> str:
    """
    Convierte una fracción decimal (0.030447) al formato chileno de porcentaje
    usado en las plantillas de carga: "3,04%".
    """
    try:
        num = float(str(valor).strip())
    except (ValueError, TypeError):
        return ""
    return f"{num * 100:.2f}".replace(".", ",") + "%"


# ─────────────────────────────────────────────
# NOMBRES
# ─────────────────────────────────────────────

def concatenar_nombre(row, col_nombre: str, col_ap: str, col_am: str) -> str:
    """
    Equivale a: =CONCATENAR(C2;" ";D2;" ";E2)
    Une nombre + apellido paterno + apellido materno con espacios.
    """
    partes = [
        str(row.get(col_nombre, "")).strip(),
        str(row.get(col_ap, "")).strip(),
        str(row.get(col_am, "")).strip(),
    ]
    return " ".join(p for p in partes if p and p != "nan")


# ─────────────────────────────────────────────
# REPETIDOS
# ─────────────────────────────────────────────

def separar_repetidos(df: pd.DataFrame, col_rut: str, ruts_bd: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Equivale al BUSCARV para detectar repetidos.

    Retorna:
        (df_nuevos, df_repetidos)
    - df_nuevos: registros que NO están en la BD → van a la carga
    - df_repetidos: registros que SÍ están en la BD → van al archivo de repetidos
    """
    df = df.copy()
    df["_rut_str"] = df[col_rut].astype(str).str.strip()
    mask_repetido = df["_rut_str"].isin(ruts_bd)

    df_repetidos = df[mask_repetido].drop(columns=["_rut_str"])
    df_nuevos = df[~mask_repetido].drop(columns=["_rut_str"])

    return df_nuevos, df_repetidos


# ─────────────────────────────────────────────
# LISTA NEGRA
# ─────────────────────────────────────────────

def separar_lista_negra(
    df: pd.DataFrame,
    col_telefono: str,
    lista_negra: set
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Equivale al BUSCARV contra la lista negra.

    Retorna:
        (df_limpios, df_bloqueados)
    - df_limpios: registros cuyo teléfono NO está en lista negra → van a carga
    - df_bloqueados: registros cuyo teléfono SÍ está en lista negra → van a bloqueo
    """
    df = df.copy()
    # Normalizar fono para cruce: quitar el 0 inicial si existe
    # (en PostgreSQL los fonos se guardan sin 0, ej: 945595925)
    def _normalizar(f):
        f = str(f).strip().replace(".0", "")
        return f[1:] if f.startswith("0") else f

    df["_fono_check"] = df[col_telefono].apply(_normalizar)
    mask_bloqueado = df["_fono_check"].isin(lista_negra)

    df_bloqueados = df[mask_bloqueado].copy()
    # Agregar columna que indica cuál fono estaba bloqueado
    df_bloqueados["FONO_BLOQUEADO"] = df_bloqueados["_fono_check"]
    df_bloqueados = df_bloqueados.drop(columns=["_fono_check"])

    df_limpios = df[~mask_bloqueado].drop(columns=["_fono_check"])

    return df_limpios, df_bloqueados


# ─────────────────────────────────────────────
# CONTACTOS EFECTIVOS
# ─────────────────────────────────────────────

def aplicar_contacto_efectivo(
    df: pd.DataFrame,
    col_rut: str,
    col_telefono_destino: str,
    contactos_efectivos: dict
) -> pd.DataFrame:
    """
    Equivale al BUSCARV(A2;Hoja1!C:D;2;0) para contactos efectivos 5757.

    Si el RUT tiene un contacto efectivo registrado, reemplaza el teléfono
    principal con el número gestionado exitosamente.
    """
    df = df.copy()

    def reemplazar_si_efectivo(row):
        rut = str(row[col_rut]).strip()
        if rut in contactos_efectivos:
            return str(contactos_efectivos[rut]).strip()
        return row[col_telefono_destino]

    df[col_telefono_destino] = df.apply(reemplazar_si_efectivo, axis=1)
    return df

# ─────────────────────────────────────────────
# NOMBRES DE ARCHIVO SIN COLISIÓN
# ─────────────────────────────────────────────

def nombre_sin_colision(ruta: str) -> str:
    """
    Siempre agrega sufijo de turno según hora:
    - Mañana (antes de 13:00): AM, AM2, AM3, ...
    - Tarde  (desde 13:00):    PM, PM2, PM3, ...
    """
    import os
    from datetime import datetime

    base, ext = os.path.splitext(ruta)
    turno = "AM" if datetime.now().hour < 13 else "PM"

    candidato = f"{base}{turno}{ext}"
    if not os.path.exists(candidato):
        return candidato

    n = 2
    while True:
        candidato = f"{base}{turno}{n}{ext}"
        if not os.path.exists(candidato):
            return candidato
        n += 1

# ─────────────────────────────────────────────
# EXPORTAR A MÚLTIPLES DESTINOS (compartida / local)
# ─────────────────────────────────────────────

def exportar_multi_destino(
    tareas: list[tuple],
    dirs: dict,
    claves_local: set[str] = frozenset({"carga", "bloqueo"}),
) -> dict[str, str]:
    """
    Exporta cada DataFrame a los destinos que correspondan según `dirs`.

    - `tareas`: lista de (df, nombre_archivo, sheet_name, reprocesar, clave)
      donde `clave` identifica el archivo (ej: "carga", "bloqueo", "repetidos").
    - `dirs`: dict con "compartida" y/o "local" -> ruta de carpeta
      (viene de main.get_output_dirs). Puede faltar alguna de las dos claves.
    - `claves_local`: qué archivos (por `clave`) se guardan también en "local".
      Por defecto solo Carga y Bloqueo (los únicos necesarios en local).

    Reglas:
      - Si hay carpeta "compartida": TODOS los archivos se guardan ahí.
      - Si hay carpeta "local": SOLO los de `claves_local` se guardan ahí.
      - Si no hay ninguna de las dos, se usa "/tmp" (fallback, como antes).

    Retorna {clave: path_usado} para armar la respuesta del endpoint,
    priorizando la ruta de "compartida" cuando el archivo se guardó en ambas.
    """
    carpeta_compartida = dirs.get("compartida")
    carpeta_local = dirs.get("local")
    if not carpeta_compartida and not carpeta_local:
        carpeta_compartida = "/tmp"

    trabajos = []          # (df, path, sheet, reprocesar) a ejecutar
    paths_resultado = {}   # clave -> path representativo

    for df, nombre, sheet, reprocesar, clave in tareas:
        path_usado = None
        if carpeta_compartida:
            path_c = nombre_sin_colision(f"{carpeta_compartida}/{nombre}")
            trabajos.append((df, path_c, sheet, reprocesar))
            path_usado = path_c
        if carpeta_local and clave in claves_local:
            path_l = nombre_sin_colision(f"{carpeta_local}/{nombre}")
            trabajos.append((df, path_l, sheet, reprocesar))
            if path_usado is None:
                path_usado = path_l
        paths_resultado[clave] = path_usado

    if trabajos:
        from concurrent.futures import ThreadPoolExecutor as _TPE
        with _TPE(max_workers=max(4, len(trabajos))) as pool:
            pool.map(lambda t: exportar_excel(t[0], t[1], sheet_name=t[2], reprocesar=t[3]), trabajos)

    return paths_resultado


# ─────────────────────────────────────────────
# EXPORTAR A EXCEL
# ─────────────────────────────────────────────

def exportar_excel(df: pd.DataFrame, path: str, sheet_name: str = "Contactos", reprocesar: bool = True):
    """
    Exporta DataFrame a .xls (97-2003) con formato exacto de archivo de carga.
    - sheet_name: "Contactos" (default) o "ESTADO" para bloqueos
    - Si el DataFrame está vacío, no genera el archivo.
    """
    import os
    import xlwt
    from xlwt import Workbook as XlsWorkbook

    # No generar archivo si no hay datos
    if df is None or len(df) == 0:
        print(f"⏭️  Sin datos, archivo no generado: {os.path.basename(path)}")
        return

    # Forzar extensión .xls
    base, ext = os.path.splitext(path)
    path = base + ".xls"

    wb = XlsWorkbook(encoding="latin1")
    ws = wb.add_sheet(sheet_name)

    # ── Estilos ──────────────────────────────────────────────────
    font_header = xlwt.Font(); font_header.name = "Verdana"; font_header.height = 160; font_header.bold = True
    wb.set_colour_RGB(0x20, 0xD1, 0xD1, 0xD1)
    pattern_header = xlwt.Pattern(); pattern_header.pattern = xlwt.Pattern.SOLID_PATTERN; pattern_header.pattern_fore_colour = 0x20
    style_header = xlwt.XFStyle(); style_header.font = font_header; style_header.pattern = pattern_header

    font_data = xlwt.Font(); font_data.name = "Arial"; font_data.height = 200
    style_normal = xlwt.XFStyle(); style_normal.font = font_data
    style_texto = xlwt.XFStyle(); style_texto.font = font_data; style_texto.num_format_str = "@"

    cols_tel = {c for c in df.columns if any(k in c.lower() for k in ("tel", "fono", "celular"))}

    # ── Encabezados ─────────────────────────────────────────────
    for col_idx, col_name in enumerate(df.columns):
        ws.write(0, col_idx, col_name, style_header)
        tiene_datos = df[col_name].astype(str).str.strip().replace("", pd.NA).notna().any()
        ws.col(col_idx).width = max(len(str(col_name)) * 300, 3000) if tiene_datos else 800

    # ── Datos ────────────────────────────────────────────────────
    for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
        for col_idx, col_name in enumerate(df.columns):
            raw = row[col_name]
            if raw is None or (isinstance(raw, float) and str(raw) in ("nan", "inf")):
                value = ""
            elif str(raw).strip() == "99999":
                value = 99999
            elif col_name in cols_tel:
                value = str(raw).strip()
            else:
                value = str(raw).strip().replace("\x00", "").replace("\r", "") if isinstance(raw, str) else raw

            if col_name in ("Orden Discado", "OrdenDiscado") and value != "":
                try:
                    value = int(str(value).strip())
                except (ValueError, TypeError):
                    pass

            ws.write(row_idx, col_idx, value, style_texto if col_name in cols_tel else style_normal)

    # Si el archivo está abierto en Excel, guardar con nombre alternativo
    if os.path.exists(path):
        try:
            os.rename(path, path)
        except OSError:
            path = f"{base}-nuevo.xls"
            print(f"⚠️  Archivo ocupado, guardando como: {os.path.basename(path)}")

    wb.save(path)
    print(f"✅ Archivo generado: {os.path.basename(path)}")

    # Reprocesar con xlwings solo si se indica
    if not reprocesar:
        return

    import threading
    _com_lock = exportar_excel.__dict__.setdefault("_com_lock", threading.Lock())

    try:
        import xlwings as xw
        abs_path = os.path.abspath(path)
        with _com_lock:
            with xw.App(visible=False, add_book=False) as app:
                app.display_alerts = False
                app.screen_updating = False
                wb_xw = app.books.open(abs_path)
                wb_xw.save()
                wb_xw.close()
        print(f"✅ Reprocesado con xlwings: {os.path.basename(path)}")
    except Exception as e:
        import traceback
        print(f"⚠️  xlwings error: {traceback.format_exc()}")


def _numero_romano(n: int) -> str:
    """
    Convierte un entero positivo a número romano en mayúsculas (I, II, III, IV, ...).
    """
    valores = [
        (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
        (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
        (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"),
    ]
    resultado = []
    for valor, simbolo in valores:
        while n >= valor:
            resultado.append(simbolo)
            n -= valor
    return "".join(resultado)


def exportar_excel_particionado(
    df: pd.DataFrame,
    path_base: str,
    sheet_name: str = "Sheet1",
    max_filas: int = 60_000,
    reprocesar: bool = True,
) -> list[str]:
    """
    Exporta un DataFrame a uno o más .xls, partiendo cada `max_filas` registros.
    El formato .xls (97-2003) tiene un tope físico de 65.536 filas por hoja;
    se usa un margen bajo ese tope para no rozar el límite.

    Si el DataFrame cabe en un solo archivo, genera `path_base` sin sufijo.
    Si hay que partirlo, calcula cuántas partes hacen falta según `max_filas`
    y reparte las filas PAREJO entre ellas (no llena una al tope y deja el
    resto en la última). Genera `...I.xls`, `...II.xls`, `...III.xls`, etc.
    """
    import os

    if df is None or len(df) == 0:
        return []

    base, ext = os.path.splitext(path_base)
    ext = ext or ".xls"

    total = len(df)

    if total <= max_filas:
        exportar_excel(df, path_base, sheet_name=sheet_name, reprocesar=reprocesar)
        return [base + ".xls"]

    # Cantidad de partes necesarias según el tope, y luego reparto PAREJO
    # entre esas partes (no "llenar al tope y dejar el resto al final").
    # Ej: 132.900 filas con max_filas=60.000 -> 3 partes de 44.300 c/u,
    # en vez de 60.000 + 60.000 + 12.900.
    n_partes = (total + max_filas - 1) // max_filas
    base_filas = total // n_partes
    resto = total % n_partes
    tamanos = [base_filas + 1 if i < resto else base_filas for i in range(n_partes)]

    rutas = []
    inicio = 0
    for i, tam in enumerate(tamanos):
        bloque = df.iloc[inicio : inicio + tam]
        ruta_parte = f"{base}{_numero_romano(i + 1)}{ext}"
        exportar_excel(bloque, ruta_parte, sheet_name=sheet_name, reprocesar=reprocesar)
        rutas.append(os.path.splitext(ruta_parte)[0] + ".xls")
        inicio += tam
    return rutas