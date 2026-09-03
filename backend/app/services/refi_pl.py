import io
import pandas as pd
from datetime import date, datetime
from app.services.utils import (
    agregar_cero,
    concatenar_nombre,
    separar_repetidos,
    exportar_excel,
    exportar_multi_destino,
    exportar_txt_carga,
    extraer_horario_archivo,
    leer_archivo,
    nombre_sin_colision,
)
from app.core.sqlserver import get_repetidos
from app.core.postgres import registrar_log, registrar_repetidos
from app.core.ftp import descargar_archivo_sftp


# ─────────────────────────────────────────────
# LAYOUT EXACTO DEL TXT DE CARGA (el que se sube por FTP/FileZilla)
# Armado a partir de los archivos de ejemplo Salida*Leakage*.txt
# ─────────────────────────────────────────────

COLUMNAS_TXT_REFI = [
    "RUT", "DIGITO", "NOMBRE", "APELLIDO_PATERNO", "APELLIDO_MATERNO",
    "FECHA_DE_NACIMIENTO", "DIRECCION_PARTICULAR", "COMUNA_PARTICULAR", "CIUDAD_PARTICULAR",
    "PAG009", "OBM011", "OBM015", "SUCURSAL_DEUDOR",
    "TELEFONO1", "TELEFONO2", "TELEFONO3", "TELEFONO4", "TELEFONO5", "TELEFONO6",
    "TELEFONO_PARTICULAR", "TELEFONO8", "TELEFONO9", "TELEFONO10", "TELEFONO11",
    "PORCENTAJE_DEUDA", "TIPO_PROPENSION", "MARCA_PROPENSION", "PIE", "RANGO",
    "CLASIFICACION", "ORDEN_DISCADO", "TIPOBASE", "FECHAVCTO", "PRODUCTO",
    "OBM019", "OBM020", "TASA", "OBM012", "OBM013", "FECHA_CARGA", "IDENTIF_DEUDOR",
    "CODIGO_SEXO", "ESTADO_CIVIL", "EMPRESA_DEL_DEUDOR", "CARGO_DEL_DEUDOR",
    "PROFESION_DEL_DEUDOR", "PAIS_PARTICULAR", "CODIGO_COMUNA_PART",
    "DIRECCION_COMERCIAL", "COMUNA_COMERCIAL", "CIUDAD_COMERCIAL", "PAIS_COMERCIAL",
    "PAG007", "OBM004", "PAG014", "PAG017", "DCTO_TASA",
]

COLUMNAS_TXT_PL = [
    "RUT", "DIGITO", "NOMBRE", "APELLIDO_PATERNO", "APELLIDO_MATERNO",
    "FECHA_DE_NACIMIENTO", "DIRECCION_PARTICULAR", "COMUNA_PARTICULAR", "CIUDAD_PARTICULAR",
    "PAG009", "OBM011", "OBM015", "SUCURSAL_DEUDOR",
    "TELEFONO1", "TELEFONO2", "TELEFONO3", "TELEFONO4", "TELEFONO5", "TELEFONO6",
    "TELEFONO7", "TELEFONO8", "TELEFONO9", "TELEFONO10", "TELEFONO11",
    "PORCENTAJE_DEUDA", "TIPO_PROPENSION", "MARCA_PROPENSION", "PIE", "RANGO",
    "CLASIFICACION", "ORDEN_DISCADO", "TIPOBASE", "FECHAVCTO", "PRODUCTO",
    "OBM019", "OBM020", "OBM012", "OBM013", "TASA", "BDD", "FECHA_CARGA",
    "DESCUENTO_TASA", "MARCA_ESTRATEGIA", "PROPENSION", "AV", "SAV", "NOVEDAD",
]

# REFI y PL no necesitan alias: los nombres del builder (Rut, Digito,
# Apellido_Paterno, "Telefono 1", "Fecha Carga", etc.) ya normalizan
# exactamente al layout del TXT de carga.
ALIAS_TXT_REFI: dict[str, str] = {}
ALIAS_TXT_PL: dict[str, str] = {}

# Nombre del TXT de carga: misma estructura que los archivos de ejemplo
# (Salida{Tipo}Leakage{AAAAMMDDHHMM}.txt), sin sufijo de turno AM/PM.
PREFIJO_TXT_CARGA = {"REFI": "SalidaRefiLeakage", "PL": "SalidaPagoLivianoLeakage"}


def _col(df, col, default=""):
    if col in df.columns:
        return df[col].fillna("").tolist()
    # Fallback: si piden col con _x y no existe, buscar sin sufijo
    if col.endswith("_x"):
        base = col[:-2]
        if base in df.columns:
            return df[base].fillna("").tolist()
    return [default] * len(df)


def procesar_refi_pl(
    tipo: str,
    output_dirs: dict = None,
    archivo_bytes: bytes = None,
    nombre_archivo: str = None,
    progress_cb=None,
    usuario: str = "",
) -> dict:
    """
    Procesa REFI o PL Leakage.
    Si no se pasa archivo_bytes, descarga automáticamente el más reciente del SFTP.

    Rutas FTP de resoluciones:
      REFI → ftp://...DOWNLOAD/Resultante_PL/RNXXXXXXXX.TXT
      PL   → ftp://...DOWNLOAD/Resultante_PL/XXXXXXXX.TXT
    """
    def emit(step):
        if progress_cb:
            progress_cb(step)

    output_dirs = output_dirs or {}

    hoy = date.today().strftime("%Y%m%d")
    fecha_carga = date.today().strftime("%d/%m/%Y")
    tipo = tipo.upper()

    if tipo not in ("REFI", "PL"):
        raise ValueError("tipo debe ser 'REFI' o 'PL'")

    # 1. Obtener archivo desde SFTP o el subido manualmente
    if archivo_bytes is None:
        emit("Descargando desde SFTP")
        archivo_bytes, nombre_archivo = descargar_archivo_sftp(tipo)
        emit(f"Archivo: {nombre_archivo}")

    # 2. Leer archivo
    emit("Leyendo archivo")
    df = leer_archivo(archivo_bytes, nombre_archivo)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)
    df_original = df.copy()

    # 3. Separar repetidos
    caso_bd = "REFI" if tipo == "REFI" else "PL"
    emit("Verificando repetidos")
    ruts_repetidos = get_repetidos(caso_bd)
    df_nuevos, df_repetidos = separar_repetidos(df, "RUT", ruts_repetidos)
    df_nuevos = df_nuevos.reset_index(drop=True)
    df_repetidos = df_repetidos.reset_index(drop=True)

    # 4. Filtrar por ESTADO_OFERTA (REFI) o DEUDA_ACUERDO >= $300.000 (PL)
    emit("Aplicando filtro de oferta")
    if tipo == "REFI":
        if "ESTADO_OFERTA" in df_nuevos.columns:
            mask_sin_oferta = df_nuevos["ESTADO_OFERTA"].astype(str).str.strip() == "SIN OFERTA"
            df_excluidos_oferta = df_nuevos[mask_sin_oferta].reset_index(drop=True)
            df_nuevos = df_nuevos[~mask_sin_oferta].reset_index(drop=True)
        else:
            print("[WARN] Columna ESTADO_OFERTA no encontrada en el archivo REFI, se omite filtro")
            df_excluidos_oferta = pd.DataFrame()
    else:  # PL
        if "DEUDA_ACUERDO" in df_nuevos.columns:
            monto = pd.to_numeric(df_nuevos["DEUDA_ACUERDO"], errors="coerce").fillna(0)
            mask_monto = monto >= 300_000
            df_excluidos_oferta = df_nuevos[~mask_monto].reset_index(drop=True)
            df_nuevos = df_nuevos[mask_monto].reset_index(drop=True)
        else:
            print("[WARN] Columna DEUDA_ACUERDO no encontrada en el archivo PL, se omite filtro")
            df_excluidos_oferta = pd.DataFrame()

    # 4b. Sin blacklist para REFI/PL
    df_bloqueados = pd.DataFrame()

    # 5. Filtrar registros con monto menor a $700.000 (si aplica)
    # (sin cambios, no existe en REFI/PL original, se mantiene igual)

    # 6. Cruzar resoluciones pendientes ANTES de construir carga y bloqueo
    emit("Cruzando agendas (SQL Server)")
    try:
        from app.core.sqlserver import get_ruts_agendados
        ruts_agendados = get_ruts_agendados(tipo)
        mask_agendas = df_nuevos["RUT"].astype(str).str.strip().isin(ruts_agendados)
        df_resoluciones = df_nuevos[mask_agendas].reset_index(drop=True)
        total_resoluciones = len(df_resoluciones)
    except Exception as e:
        print(f"[WARN] Error al consultar agendas SQL Server, se omite: {e}")
        df_resoluciones = pd.DataFrame()
        total_resoluciones = 0

    # ── Excluir agendas FTP de df_nuevos ANTES de construir carga y bloqueo ──
    if not df_resoluciones.empty and "RUT" in df_resoluciones.columns:
        ruts_agendas = set(df_resoluciones["RUT"].astype(str).str.strip())
        mask_no_agenda = ~df_nuevos["RUT"].astype(str).str.strip().isin(ruts_agendas)
        df_nuevos = df_nuevos[mask_no_agenda].reset_index(drop=True)

    # 7. Construir archivo de carga (df_nuevos ya no incluye agendas FTP ni excluidos por oferta)
    if tipo == "REFI":
        df_carga = _construir_carga_refi(df_nuevos, fecha_carga)
        nombre_carga        = f"CargaRefiLeakage{hoy}.xls"
        nombre_repetidos    = f"RegistrosRepetidosREFILeakage{hoy}.xls"
        nombre_bloqueo      = f"BloqueoREFILeakage{hoy}.xls"
        nombre_resoluciones = f"AgendasREFILeakage{hoy}.xls"
        nombre_excluidos    = f"ExcluidosOfertaREFILeakage{hoy}.xls"
    else:
        df_carga = _construir_carga_pl(df_nuevos, fecha_carga)
        nombre_carga        = f"CargaPagoLivianoLeakage{hoy}.xls"
        nombre_repetidos    = f"RegistrosRepetidosPLLeakage{hoy}.xls"
        nombre_bloqueo      = f"BloqueoPLLeakage{hoy}.xls"
        nombre_resoluciones = f"AgendasPLLeakage{hoy}.xls"
        nombre_excluidos    = f"ExcluidosDeudaAcuerdoPLLeakage{hoy}.xls"

    # Bloqueo: solo RUTs de los que van a carga (sin agendas)
    df_bloqueo = pd.DataFrame({"RUT": _col(df_nuevos, "RUT")})

    # 8. Generar PRIMERO el archivo de carga en TXT (el que se sube al sistema)
    #    y subirlo por FTP/SFTP, antes de generar y copiar los .xls a las
    #    carpetas compartida/local.
    emit("Generando archivo de carga en TXT")
    carpeta_txt = output_dirs.get("compartida") or output_dirs.get("local") or "/tmp"
    horario_txt = extraer_horario_archivo(nombre_archivo or "") or datetime.now().strftime("%H%M")
    nombre_carga_txt = f"{PREFIJO_TXT_CARGA[tipo]}{hoy}{horario_txt}.txt"
    path_carga_txt = f"{carpeta_txt}/{nombre_carga_txt}"
    columnas_txt = COLUMNAS_TXT_REFI if tipo == "REFI" else COLUMNAS_TXT_PL
    alias_txt = ALIAS_TXT_REFI if tipo == "REFI" else ALIAS_TXT_PL
    path_carga_txt = exportar_txt_carga(df_carga, path_carga_txt, columnas_txt, alias=alias_txt)

    if path_carga_txt:
        emit("Subiendo TXT de carga por FTP/SFTP")
        try:
            from app.core.ftp_neotel17 import subir_archivo_carga_txt
            subir_archivo_carga_txt(path_carga_txt, tipo=tipo)
        except Exception as e:
            print(f"⚠️  Error subiendo TXT por FTP/SFTP: {e}")

    # 9. Exportar: TODO va a "compartida"; solo Carga y Bloqueo van también a "local"
    emit("Generando archivos Excel")
    tareas = [
        (df_carga,            nombre_carga,        "Contactos", True,  "carga"),
        (df_repetidos,        nombre_repetidos,    "Contactos", False, "repetidos"),
        (df_bloqueo,          nombre_bloqueo,      "ESTADO",    True,  "bloqueo"),
        (df_resoluciones,     nombre_resoluciones, "Contactos", False, "resoluciones"),
        (df_excluidos_oferta, nombre_excluidos,    "Contactos", False, "excluidos"),
    ]
    paths = exportar_multi_destino(tareas, output_dirs, claves_local={"carga", "bloqueo"})
    path_carga        = paths["carga"]
    path_repetidos    = paths["repetidos"]
    path_bloqueo      = paths["bloqueo"]
    path_resoluciones = paths["resoluciones"]
    path_excluidos    = paths["excluidos"]

    # 10. Log
    registrar_log(
        tipo_caso=tipo,
        total_entrada=total_entrada,
        total_repetidos=len(df_repetidos),
        total_bloqueados=len(df_bloqueados),
        total_carga=len(df_carga),
        archivo_origen=nombre_archivo,
        usuario=usuario,
    )
    if len(df_repetidos) > 0:
        registrar_repetidos(
            ruts=df_repetidos["RUT"].astype(str).str.strip().tolist(),
            tipo_caso=tipo,
        )

    # 11. Marcar en snapshot del watcher para que el panel muestre "✓ Procesado"
    try:
        from app.core.ftp_watcher import _marcar_procesado, _clave_procesado, _extraer_horario
        horario = _extraer_horario(nombre_archivo or "")
        _marcar_procesado(_clave_procesado(tipo, horario, nombre_archivo or ""))
    except Exception as _e:
        pass

    # 12. Confirmar contra la BD de Neotel que la carga subida por FTP
    #     efectivamente quedó insertada (espera + reintentos; ver
    #     app.core.confirmacion_carga). Corre en segundo plano (no
    #     bloquea este worker ~90s): el resultado queda en Postgres
    #     (log_confirmacion_carga) y, si algo no confirma, se avisa por
    #     Teams.
    try:
        from app.core.confirmacion_carga import confirmar_carga_en_segundo_plano
        confirmar_carga_en_segundo_plano(
            caso=tipo,
            valores=_col(df_carga, "Rut"),
            archivo_origen=nombre_archivo,
            usuario=usuario,
        )
    except Exception as e:
        print(f"⚠️  Error iniciando confirmación de carga {tipo} en Neotel: {e}")

    return {
        "archivo_carga":        path_carga,
        "archivo_carga_txt":    path_carga_txt,
        "archivo_repetidos":    path_repetidos,
        "archivo_bloqueo":      path_bloqueo,
        "archivo_resoluciones": path_resoluciones,
        "archivo_excluidos":    path_excluidos,
        "total_entrada":        total_entrada,
        "total_repetidos":      len(df_repetidos),
        "total_carga":          len(df_carga),
        "total_resoluciones":   total_resoluciones,
        "total_excluidos":      len(df_excluidos_oferta),
        "_archivo_bytes":       archivo_bytes,
        "_nombre_archivo":      nombre_archivo,
        "_caso_confirmacion":     tipo,
        "_columna_confirmacion":  "TXTRUT",
        "_valores_confirmacion":  _col(df_carga, "Rut"),
    }


def _get_telefonos(df, prefijos):
    """Retorna lista de teléfonos con cero, probando distintos nombres de columna."""
    for col in prefijos:
        if col in df.columns:
            return [agregar_cero(v) for v in _col(df, col)]
    return ["00"] * len(df)


def _construir_carga_refi(df: pd.DataFrame, fecha_carga: str) -> pd.DataFrame:
    n = len(df)
    cols = [
        "Rut","Digito","Nombre","Apellido_Paterno","Apellido_Materno",
        "Fecha_de_Nacimiento","Direccion_Particular","Comuna_Particular","Ciudad_Particular",
        "PAG009","OBM011","OBM015","Sucursal_Deudor",
        "Telefono 1","Teléfono 2","Teléfono 3","Teléfono 4","Teléfono 5","Teléfono 6",
        "Telefono Particular","Telefono 8","Telefono 9","Telefono 10","Telefono 11",
        "PORCENTAJE DEUDA","TipoPropension","MARCA_PROPENSION","PIE","Rango","Clasificación",
        "Orden Discado","TipoBase","FECHAVCTO","PRODUCTO","OBM019","OBM020","Tasa",
        "OBM012","OBM013","Fecha Carga","Identif_Deudor","Codigo_Sexo","Estado_Civil",
        "Empresa_del_deudor","Cargo_del_deudor","Profesion_del_deudor","Pais_Particular",
        "Codigo_Comuna_Part","Direccion_Comercial","Comuna_Comercial","Ciudad_Comercial",
        "Pais_Comercial","PAG007","OBM004","PAG014","PAG017","DCTO_TASA"
    ]
    if n == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "Rut":                  _col(df, "RUT"),
        "Digito":               _col(df, "DV"),
        "Nombre":               _col(df, "NOMBRE_x"),
        "Apellido_Paterno":     _col(df, "APELLIDO_PATERNO_x"),
        "Apellido_Materno":     _col(df, "APELLIDO_MATERNO_x"),
        "Fecha_de_Nacimiento":  [""] * n,
        "Direccion_Particular": _col(df, "DIRECCION_CUENTA_x"),
        "Comuna_Particular":    _col(df, "COMUNA_CUENTA_x"),
        "Ciudad_Particular":    _col(df, "CIUDAD_CUENTA_x"),
        "PAG009":               [""] * n,
        "OBM011":               [""] * n,
        "OBM015":               [""] * n,
        "Sucursal_Deudor":      [""] * n,
        "Telefono 1":           _get_telefonos(df, ["TELEFONO1", "TELEFONO_1"]),
        "Teléfono 2":           _get_telefonos(df, ["TELEFONO2", "TELEFONO_2"]),
        "Teléfono 3":           _get_telefonos(df, ["TELEFONO3", "TELEFONO_3"]),
        "Teléfono 4":           [""] * n,
        "Teléfono 5":           [""] * n,
        "Teléfono 6":           [""] * n,
        "Telefono Particular":  [""] * n,
        "Telefono 8":           [""] * n,
        "Telefono 9":           [""] * n,
        "Telefono 10":          [""] * n,
        "Telefono 11":          [""] * n,
        "PORCENTAJE DEUDA":     [""] * n,
        "TipoPropension":       [""] * n,
        "MARCA_PROPENSION":     [""] * n,
        "PIE":                  [""] * n,
        "Rango":                [""] * n,
        "Clasificación":        [""] * n,
        "Orden Discado":        [99999] * n,
        "TipoBase":             ["RN Leakage"] * n,
        "FECHAVCTO":            [""] * n,
        "PRODUCTO":             _col(df, "PRODUCTO_APP"),
        "OBM019":               [""] * n,
        "OBM020":               [""] * n,
        "Tasa":                 [""] * n,
        "OBM012":               [""] * n,
        "OBM013":               [""] * n,
        "Fecha Carga":          [fecha_carga] * n,
        "Identif_Deudor":       [""] * n,
        "Codigo_Sexo":          [""] * n,
        "Estado_Civil":         [""] * n,
        "Empresa_del_deudor":   [""] * n,
        "Cargo_del_deudor":     [""] * n,
        "Profesion_del_deudor": [""] * n,
        "Pais_Particular":      [""] * n,
        "Codigo_Comuna_Part":   [""] * n,
        "Direccion_Comercial":  [""] * n,
        "Comuna_Comercial":     [""] * n,
        "Ciudad_Comercial":     [""] * n,
        "Pais_Comercial":       [""] * n,
        "PAG007":               [""] * n,
        "OBM004":               [""] * n,
        "PAG014":               [""] * n,
        "PAG017":               [""] * n,
        "DCTO_TASA":            [""] * n,
    })


def _construir_carga_pl(df: pd.DataFrame, fecha_carga: str) -> pd.DataFrame:
    n = len(df)
    cols = [
        "Rut","Digito","Nombre","Apellido_Paterno","Apellido_Materno",
        "Fecha_de_Nacimiento","Direccion_Particular","Comuna_Particular","Ciudad_Particular",
        "PAG009","OBM011","OBM015","Sucursal_Deudor",
        "Telefono 1","Teléfono 2","Teléfono 3","Teléfono 4","Teléfono 5","Teléfono 6",
        "Telefono7","Telefono 8","Telefono 9","Telefono 10","Telefono 11",
        "PORCENTAJE DEUDA","TipoPropension","MARCA_PROPENSION","PIE","Rango","Clasificación",
        "Orden Discado","TipoBase","FECHAVCTO","PRODUCTO","OBM019","OBM020",
        "OBM012","OBM013","Tasa","BDD","FechaCarga","Descuento Tasa",
        "MarcaEstrategia","Propension","AV","SAV","Novedad"
    ]
    if n == 0:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame({
        "Rut":                  _col(df, "RUT"),
        "Digito":               _col(df, "DV"),
        "Nombre":               _col(df, "NOMBRE"),
        "Apellido_Paterno":     _col(df, "APELLIDO_PATERNO"),
        "Apellido_Materno":     _col(df, "APELLIDO_MATERNO"),
        "Fecha_de_Nacimiento":  [""] * n,
        "Direccion_Particular": _col(df, "DIRECCION_CUENTA_x"),
        "Comuna_Particular":    _col(df, "COMUNA_CUENTA_x"),
        "Ciudad_Particular":    _col(df, "CIUDAD_CUENTA_x"),
        "PAG009":               [""] * n,
        "OBM011":               [""] * n,
        "OBM015":               [""] * n,
        "Sucursal_Deudor":      [""] * n,
        "Telefono 1":           _get_telefonos(df, ["TELEFONO1", "TELEFONO_1"]),
        "Teléfono 2":           _get_telefonos(df, ["TELEFONO2", "TELEFONO_2"]),
        "Teléfono 3":           _get_telefonos(df, ["TELEFONO3", "TELEFONO_3"]),
        "Teléfono 4":           [""] * n,
        "Teléfono 5":           [""] * n,
        "Teléfono 6":           [""] * n,
        "Telefono7":            [""] * n,
        "Telefono 8":           [""] * n,
        "Telefono 9":           [""] * n,
        "Telefono 10":          [""] * n,
        "Telefono 11":          [""] * n,
        "PORCENTAJE DEUDA":     [""] * n,
        "TipoPropension":       [""] * n,
        "MARCA_PROPENSION":     [""] * n,
        "PIE":                  [""] * n,
        "Rango":                [""] * n,
        "Clasificación":        [""] * n,
        "Orden Discado":        [99999] * n,
        "TipoBase":             ["PL Leakage"] * n,
        "FECHAVCTO":            [""] * n,
        "PRODUCTO":             _col(df, "PRODUCTO_APP"),
        "OBM019":               [""] * n,
        "OBM020":               [""] * n,
        "OBM012":               [""] * n,
        "OBM013":               [""] * n,
        "Tasa":                 [""] * n,
        "BDD":                  [""] * n,
        "FechaCarga":           [fecha_carga] * n,
        "Descuento Tasa":       [""] * n,
        "MarcaEstrategia":      [""] * n,
        "Propension":           [""] * n,
        "AV":                   [""] * n,
        "SAV":                  [""] * n,
        "Novedad":              [""] * n,
    })