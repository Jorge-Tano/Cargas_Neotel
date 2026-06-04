import io
import pandas as pd
from datetime import date
from app.services.utils import (
    agregar_cero,
    concatenar_nombre,
    separar_repetidos,
    exportar_excel,
    leer_archivo,
    nombre_sin_colision,
)
from app.core.sqlserver import get_repetidos
from app.core.postgres import registrar_log, registrar_repetidos
from app.core.ftp import descargar_archivo_sftp
from app.services.resoluciones import procesar_resoluciones_pendientes


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
    output_dir: str = "/tmp",
    archivo_bytes: bytes = None,
    nombre_archivo: str = None,
    progress_cb=None,
    usuario: str = "",
    txt_resoluciones_bytes: bytes = None,
    txt_resoluciones_nombre: str = None,
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

    # 4. Sin blacklist para REFI/PL
    df_bloqueados = pd.DataFrame()

    # 5. Filtrar registros con monto menor a $700.000 (si aplica)
    # (sin cambios, no existe en REFI/PL original, se mantiene igual)

    # 6. Cruzar resoluciones pendientes ANTES de construir carga y bloqueo
    emit("Cruzando resoluciones pendientes (FTP)")
    try:
        df_resoluciones, total_resoluciones = procesar_resoluciones_pendientes(
            df_base=df_nuevos,
            col_rut_base="RUT",
            tipo=tipo,
            contenido_txt=txt_resoluciones_bytes,
            nombre_txt=txt_resoluciones_nombre,
        )
    except FileNotFoundError as e:
        print(f"[WARN] TXT resoluciones no encontrado, se omite: {e}")
        df_resoluciones = pd.DataFrame()
        total_resoluciones = 0
    except Exception as e:
        print(f"[WARN] Error al procesar resoluciones, se omite: {e}")
        df_resoluciones = pd.DataFrame()
        total_resoluciones = 0

    # ── Excluir agendas FTP de df_nuevos ANTES de construir carga y bloqueo ──
    if not df_resoluciones.empty and "RUT" in df_resoluciones.columns:
        ruts_agendas = set(df_resoluciones["RUT"].astype(str).str.strip())
        mask_no_agenda = ~df_nuevos["RUT"].astype(str).str.strip().isin(ruts_agendas)
        df_nuevos = df_nuevos[mask_no_agenda].reset_index(drop=True)

    # 7. Construir archivo de carga (df_nuevos ya no incluye agendas FTP)
    if tipo == "REFI":
        df_carga = _construir_carga_refi(df_nuevos, fecha_carga)
        nombre_carga        = f"CargaRefiLeakage{hoy}.xls"
        nombre_repetidos    = f"RegistrosRepetidosREFILeakage{hoy}.xls"
        nombre_bloqueo      = f"BloqueoREFILeakage{hoy}.xls"
        nombre_resoluciones = f"AgendasREFILeakage{hoy}.xls"
    else:
        df_carga = _construir_carga_pl(df_nuevos, fecha_carga)
        nombre_carga        = f"CargaPagoLivianoLeakage{hoy}.xls"
        nombre_repetidos    = f"RegistrosRepetidosPLLeakage{hoy}.xls"
        nombre_bloqueo      = f"BloqueoPLLeakage{hoy}.xls"
        nombre_resoluciones = f"AgendasPLLeakage{hoy}.xls"

    # Bloqueo: solo RUTs de los que van a carga (sin agendas)
    df_bloqueo = pd.DataFrame({"RUT": _col(df_nuevos, "RUT")})

    # 8. Exportar en paralelo
    emit("Generando archivos Excel")
    path_carga        = nombre_sin_colision(f"{output_dir}/{nombre_carga}")
    path_repetidos    = nombre_sin_colision(f"{output_dir}/{nombre_repetidos}")
    path_bloqueo      = nombre_sin_colision(f"{output_dir}/{nombre_bloqueo}")
    path_resoluciones = nombre_sin_colision(f"{output_dir}/{nombre_resoluciones}")

    from concurrent.futures import ThreadPoolExecutor as _TPE
    tareas = [
        (df_carga,        path_carga,        "Contactos", True),
        (df_repetidos,    path_repetidos,     "Contactos", False),
        (df_bloqueo,      path_bloqueo,       "ESTADO",    True),
        (df_resoluciones, path_resoluciones,  "Contactos", False),
    ]
    with _TPE(max_workers=4) as pool:
        pool.map(lambda t: exportar_excel(t[0], t[1], sheet_name=t[2], reprocesar=t[3]), tareas)

    # 9. Log
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

    return {
        "archivo_carga":        path_carga,
        "archivo_repetidos":    path_repetidos,
        "archivo_bloqueo":      path_bloqueo,
        "archivo_resoluciones": path_resoluciones,
        "total_entrada":        total_entrada,
        "total_repetidos":      len(df_repetidos),
        "total_carga":          len(df_carga),
        "total_resoluciones":   total_resoluciones,
        "_archivo_bytes":       archivo_bytes,
        "_nombre_archivo":      nombre_archivo,
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