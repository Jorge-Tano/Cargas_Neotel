import io
import pandas as pd
import re


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
    Si el archivo ya existe, agrega sufijo _2, _3, etc.
    Ej: CargaSavLeakage20260302.xls → CargaSavLeakage20260302_2.xls
    """
    import os
    if not os.path.exists(ruta):
        return ruta
    base, ext = os.path.splitext(ruta)
    n = 2
    while os.path.exists(f"{base}_{n}{ext}"):
        n += 1
    return f"{base}_{n}{ext}"

# ─────────────────────────────────────────────
# EXPORTAR A EXCEL
# ─────────────────────────────────────────────

def exportar_excel(df: pd.DataFrame, path: str):
    """
    Exporta DataFrame a .xls (97-2003) con formato exacto de archivo de carga:
    - Encabezados: Verdana 8, negrita, fondo gris #D1D1D1
    - Datos: Arial 10
    - Teléfonos: formato texto (sin triángulo verde)
    - Limpieza automática de espacios/caracteres ocultos (reemplaza paso de Notepad++)
    - Valores 99999 se preservan tal cual
    """
    import xlwt
    from xlwt import Workbook as XlsWorkbook

    wb = XlsWorkbook(encoding="latin1")
    ws = wb.add_sheet("Contactos")

    # ── Estilo encabezado: Verdana 8, negrita, fondo gris D1D1D1 ──
    font_header = xlwt.Font()
    font_header.name = "Verdana"
    font_header.height = 160  # 8pt = 160 en xlwt
    font_header.bold = True

    pattern_header = xlwt.Pattern()
    pattern_header.pattern = xlwt.Pattern.SOLID_PATTERN
    pattern_header.pattern_fore_colour = 0x16  # gris - se sobreescribe con colour_map

    style_header = xlwt.XFStyle()
    style_header.font = font_header

    # Color gris D1D1D1 usando paleta personalizada
    wb.set_colour_RGB(0x20, 0xD1, 0xD1, 0xD1)
    pattern_header.pattern_fore_colour = 0x20
    style_header.pattern = pattern_header

    # ── Estilo datos normal: Arial 10 ──────────────────────────
    font_data = xlwt.Font()
    font_data.name = "Arial"
    font_data.height = 200  # 10pt

    style_normal = xlwt.XFStyle()
    style_normal.font = font_data

    # ── Estilo teléfono: Arial 10 + formato texto ───────────────
    style_texto = xlwt.XFStyle()
    style_texto.font = font_data
    style_texto.num_format_str = "@"

    # Detectar columnas de teléfono
    cols_tel = {c for c in df.columns if any(k in c.lower() for k in ("tel", "fono", "celular"))}

    # ── Encabezados ─────────────────────────────────────────────
    for col_idx, col_name in enumerate(df.columns):
        ws.write(0, col_idx, col_name, style_header)
        # Columnas con datos: ancho proporcional al nombre
        # Columnas vacías: ancho mínimo de 4 caracteres (800)
        tiene_datos = df[col_name].astype(str).str.strip().replace("", pd.NA).notna().any() if len(df) > 0 else False
        if tiene_datos:
            ws.col(col_idx).width = max(len(str(col_name)) * 300, 3000)
        else:
            ws.col(col_idx).width = 800  # ~4 caracteres

    # ── Datos con limpieza automática (reemplaza Notepad++) ──────
    for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
        for col_idx, col_name in enumerate(df.columns):
            raw = row[col_name]

            # Limpiar valor (equivale al copy-paste en Notepad++)
            if raw is None or (isinstance(raw, float) and str(raw) in ("nan", "inf")):
                value = ""
            elif str(raw).strip() == "99999":
                value = 99999  # Preservar 99999 como numero
            elif col_name in cols_tel:
                value = str(raw).strip()  # Telefonos siempre como texto limpio
            else:
                value = str(raw).strip().replace("\x00", "").replace("\r", "") if isinstance(raw, str) else raw

            # Orden Discado siempre como número entero
            if col_name in ("Orden Discado", "OrdenDiscado") and value != "":
                try:
                    value = int(str(value).strip())
                except (ValueError, TypeError):
                    pass

            ws.write(row_idx, col_idx, value, style_texto if col_name in cols_tel else style_normal)

    import os

    # Si el archivo está abierto en Excel, guardar con nombre alternativo
    if os.path.exists(path):
        try:
            os.rename(path, path)
        except OSError:
            base, ext = os.path.splitext(path)
            path = f"{base}_nuevo{ext}"
            print(f"⚠️  Archivo ocupado, guardando como: {os.path.basename(path)}")

    # Si el DataFrame está vacío, guardar solo con encabezados
    if len(df) == 0:
        wb_empty = XlsWorkbook(encoding="latin1")
        ws_empty = wb_empty.add_sheet("Contactos")
        for col_idx, col_name in enumerate(df.columns):
            ws_empty.write(0, col_idx, col_name, style_header)
            ws_empty.col(col_idx).width = max(len(str(col_name)) * 300, 3000)
        wb_empty.save(path)
    else:
        wb.save(path)
    print(f"✅ Archivo generado: {path}")