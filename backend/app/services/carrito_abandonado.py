"""
carrito_abandonado.py
======================
Procesa el caso "Carrito Abandonado" (Seguros / G2).

A diferencia de SAV, AV, REFI y PL:
  - El archivo de origen es un CSV (no xlsx/xls), codificado en UTF-16LE
    (con BOM), separado por comas. Llega cada hora vía SFTP en:
        {ftp_base}/{año}/SEGUROS/G2 /Carrito Abandonado/
    con nombre tipo: carrito_abandonado_202608211609.csv
                                          └─ AAAAMMDDHHMM

  - Sí cruza repetidos contra SQL Server (misma lógica que SAV/AV/PL/REFI,
    vía get_repetidos("CARRITO") → config_global: DB_CARRITO=ECRM_0035,
    IDDATABASE_CARRITO=13). No requiere lista negra ni agendas SQL Server.
    Cada corrida consulta el estado actual de la BD (no se acumulan
    exclusiones locales entre horarios: quien ya se cargó antes queda
    reflejado directamente en CONTACTOS).

Columnas de origen (CSV):
    id, Paso, Fecha, RUT, Nombre, Apellido, Correo, Telefono, Patente,
    Plan, Marca, Modelo, Año, Aseguradora, Precio_uf

Columnas de salida (carga):
    BASE, RUT, Digito, Nombre Cliente, Apellido Paterno, ApellidoMaterno,
    Tipo, Patente, Marca, Modelo, Año, NMotor, EmailCliente,
    Telefono1..Telefono5, DECILE_RANK, SEXO, Comuna, Ciudad,
    FechaNacimiento, Edad, NACIONALIDAD, ESTADO_CIVIL, HIJOS_GFAM, GSE,
    REGION_NATURAL, TipoBase, Fecha Carga, Orden_Discado, cupo,
    DESCUENTO, INSPECCION, MensajeWSP
"""

import io
import re
import pandas as pd
from datetime import date

from app.services.utils import agregar_cero, exportar_excel, exportar_multi_destino, nombre_sin_colision
from app.core.postgres import registrar_log, registrar_repetidos
from app.core.sqlserver import get_repetidos


def _reconstruir_telefono_movil(telefono_raw: str) -> str:
    """
    Los teléfonos de Carrito Abandonado llegan con 8 dígitos: se pierde
    el '9' inicial del celular chileno (ej. "32509681" en vez de
    "932509681"). Se reconstruye acá, antes de aplicar agregar_cero
    (que agrega el '0' de marcado), para llegar al formato final de
    10 dígitos: 09XXXXXXXX.
    Si el número ya viene con 9 dígitos (o cualquier otro largo/formato
    no numérico), se deja tal cual — solo se corrige el caso de 8 dígitos.
    """
    t = (telefono_raw or "").strip()
    if len(t) == 8 and t.isdigit():
        return "9" + t
    return t


def _col(df: pd.DataFrame, col: str, default: str = "") -> list:
    if col in df.columns:
        return df[col].fillna("").astype(str).tolist()
    return [default] * len(df)


def _leer_csv_carrito(archivo_bytes: bytes) -> pd.DataFrame:
    """
    Detecta encoding por BOM y lee el CSV. El archivo real llega en
    UTF-16LE, pero se deja fallback a UTF-8 por si el origen cambia.
    """
    if archivo_bytes[:2] in (b"\xff\xfe", b"\xfe\xff"):
        encoding = "utf-16"
    elif archivo_bytes[:3] == b"\xef\xbb\xbf":
        encoding = "utf-8-sig"
    else:
        encoding = "utf-8"

    texto = archivo_bytes.decode(encoding)
    df = pd.read_csv(io.StringIO(texto), dtype=str)
    df.columns = df.columns.str.strip()
    return df


_RE_RUT_DV = re.compile(r"^(.*)-([0-9kK])$")


def _split_rut(rut_raw: str) -> tuple[str, str]:
    """
    "11.209.576-4" -> ("11209576", "4")
    "6.103.688-1"  -> ("6103688", "1")
    Si no calza el patrón "cuerpo-dv", retorna el valor limpio de puntos
    como cuerpo y dv vacío.
    """
    rut_raw = (rut_raw or "").strip()
    if not rut_raw:
        return "", ""

    m = _RE_RUT_DV.match(rut_raw)
    if m:
        cuerpo, dv = m.group(1), m.group(2)
    else:
        cuerpo, dv = rut_raw, ""

    cuerpo = cuerpo.replace(".", "").strip()
    dv = dv.strip().upper()
    return cuerpo, dv


def procesar_carrito_abandonado(
    archivo_bytes: bytes,
    nombre_archivo: str,
    output_dirs: dict = None,
    progress_cb=None,
    usuario: str = "",
) -> dict:
    """
    Transforma el CSV de Carrito Abandonado al formato de carga.
    Cruza repetidos contra SQL Server (get_repetidos("CARRITO")).
    No aplica lista negra ni agendas (por diseño).
    """
    def emit(step):
        if progress_cb:
            progress_cb(step)

    output_dirs = output_dirs or {}
    fecha_carga = date.today().strftime("%d-%m-%Y")
    hoy_compacto = date.today().strftime("%Y%m%d")

    # 1. Leer archivo
    emit("Leyendo archivo CSV")
    df = _leer_csv_carrito(archivo_bytes)
    total_entrada = len(df)

    # 2. Separar RUT en cuerpo + dígito verificador
    emit("Procesando RUTs")
    ruts_split = [_split_rut(r) for r in _col(df, "RUT")]
    ruts = [r[0] for r in ruts_split]
    digitos = [r[1] for r in ruts_split]

    n = len(df)

    # 3. Construir DataFrame de carga
    emit("Construyendo archivo de carga")
    df_carga = pd.DataFrame({
        "BASE":              ["carrito_abandonado"] * n,
        "RUT":               ruts,
        "Digito":            digitos,
        "Nombre Cliente":    _col(df, "Nombre"),
        "Apellido Paterno":  _col(df, "Apellido"),
        "ApellidoMaterno":   [""] * n,
        "Tipo":              [""] * n,
        "Patente":           _col(df, "Patente"),
        "Marca":             _col(df, "Marca"),
        "Modelo":            _col(df, "Modelo"),
        "Año":               _col(df, "Año"),
        "NMotor":            [""] * n,
        "EmailCliente":      _col(df, "Correo"),
        "Telefono1":         [""] * n,  # se completa mas abajo, ya con el split sin_telefono
        "Telefono2":         [""] * n,
        "Telefono3":         [""] * n,
        "Telefono4":         [""] * n,
        "Telefono5":         [""] * n,
        "DECILE_RANK":       [""] * n,
        "SEXO":              [""] * n,
        "Comuna":            [""] * n,
        "Ciudad":            [""] * n,
        "FechaNacimiento":   [""] * n,
        "Edad":              [""] * n,
        "NACIONALIDAD":      [""] * n,
        "ESTADO_CIVIL":      [""] * n,
        "HIJOS_GFAM":        [""] * n,
        "GSE":               [""] * n,
        "REGION_NATURAL":    [""] * n,
        "TipoBase":          [""] * n,
        "Fecha Carga":       [fecha_carga] * n,
        "Orden_Discado":     [99999] * n,
        "cupo":              [""] * n,
        "DESCUENTO":         [""] * n,
        "INSPECCION":        [""] * n,
        "MensajeWSP":        [""] * n,
    })

    telefonos1 = [agregar_cero(_reconstruir_telefono_movil(v)) for v in _col(df, "Telefono")]
    df_carga["Telefono1"] = telefonos1

    # 3b. Verificar repetidos contra SQL Server (mismo patrón que SAV/AV/PL/REFI):
    #     [linked].[ECRM_0035].[dbo].[CONTACTOS/DB_CONTACTOS] con IDDATABASE=13,
    #     configurado en config_global como DB_CARRITO / IDDATABASE_CARRITO.
    emit("Verificando repetidos")
    ruts_repetidos_bd = get_repetidos("CARRITO")
    mask_repetido = pd.Series(ruts).astype(str).str.strip().isin(ruts_repetidos_bd)
    df_repetidos = df_carga[mask_repetido].reset_index(drop=True)
    df_carga     = df_carga[~mask_repetido].reset_index(drop=True)

    # 3c. Separar registros sin teléfono válido (quedan como "00" tras
    # agregar_cero cuando el CSV trae la columna Telefono vacía). No se
    # cargan: se dejan en un archivo aparte para revisión manual.
    emit("Separando registros sin teléfono")
    mask_sin_telefono = df_carga["Telefono1"].astype(str).str.strip() == "00"
    df_no_cargados = df_carga[mask_sin_telefono].reset_index(drop=True)
    df_carga       = df_carga[~mask_sin_telefono].reset_index(drop=True)

    # 4. Exportar: Carga va a compartida y a local; No Cargados solo a compartida
    emit("Generando archivo Excel")
    nombre_carga       = f"CargaCarritoAbandonado{hoy_compacto}.xls"
    nombre_repetidos   = f"RegistrosRepetidosCarritoAbandonado{hoy_compacto}.xls"
    nombre_no_cargados = f"NoCargadosCarritoAbandonado{hoy_compacto}.xls"
    tareas = [
        (df_carga,       nombre_carga,       "Contactos", True,  "carga"),
        (df_repetidos,   nombre_repetidos,   "Contactos", False, "repetidos"),
        (df_no_cargados, nombre_no_cargados, "Contactos", False, "no_cargados"),
    ]
    paths = exportar_multi_destino(tareas, output_dirs, claves_local={"carga"})
    path_carga       = paths["carga"]
    path_repetidos   = paths["repetidos"]
    path_no_cargados = paths["no_cargados"]

    # 5. Log (los "no cargados" por falta de teléfono se registran como
    #    bloqueados para que queden visibles en el log de auditoría)
    try:
        registrar_log(
            tipo_caso="CARRITO",
            total_entrada=total_entrada,
            total_repetidos=len(df_repetidos),
            total_bloqueados=len(df_no_cargados),
            total_carga=len(df_carga),
            archivo_origen=nombre_archivo,
            usuario=usuario,
        )
        if len(df_repetidos) > 0:
            registrar_repetidos(
                ruts=df_repetidos["RUT"].astype(str).str.strip().tolist(),
                tipo_caso="CARRITO",
            )
    except Exception as _e:
        print(f"[WARN] No se pudo registrar log de CARRITO: {_e}")

    # 6. Marcar en snapshot del watcher (para que el panel muestre "✓ Procesado"
    #    también cuando se procesa manualmente, no solo por el watcher)
    try:
        from app.core.ftp_watcher import _marcar_procesado, _clave_procesado, _extraer_horario
        horario = _extraer_horario(nombre_archivo or "")
        _marcar_procesado(_clave_procesado("CARRITO", horario, nombre_archivo or ""))
    except Exception:
        pass

    return {
        "archivo_carga":        path_carga,
        "archivo_repetidos":    path_repetidos,
        "archivo_no_cargados":  path_no_cargados,
        "total_entrada":        total_entrada,
        "total_carga":          len(df_carga),
        "total_no_cargados":    len(df_no_cargados),
        "total_repetidos":      len(df_repetidos),
        "total_bloqueados":     len(df_no_cargados),
        "_archivo_bytes":       archivo_bytes,
        "_nombre_archivo":      nombre_archivo,
    }