"""
carga_mensual.py
=================
Automatiza la carga mensual normal (no-Leakage) de Pago Liviano (PL) y
Refinanciamiento (REFI/RN), de punta a punta:

  1. Cruza el TXT del Resultante (FTP Neotel17, /UPLOAD/Resultante{PL,REFI} —
     lo genera la propia Tarea de Neotel que dispara
     app.core.verificador_carga_mensual) contra el Excel mensual de la
     campaña (FTP principal, /archivos/{año}/OP/{mes}) por RUT.
  2. Separa los registros que no aplican (van a eliminar.txt).
  3. Arma la(s) plantilla(s) Update (.xls, tope 65.536 filas → se parte).
  4. Arma el libro DetalleCarga con hojas "Base Cargada" / "No Cargados Comunas".
  5. Sube el/los Update(s) y el eliminar.txt a Neotel y dispara la Tarea
     "Actualizar Datos + Eliminar" (ver aplicar_en_neotel más abajo).

Crear/asociar la base y ejecutar la tarea de depósito (Resultante) ya las
automatiza app.core.verificador_carga_mensual, que además encadena todo
este módulo sin intervención humana el día que se crea la base nueva. El
endpoint /carga-mensual/{tipo}/procesar (este módulo) + /aplicar quedan
disponibles aparte para correr o reintentar el proceso a mano.
"""

from __future__ import annotations

import io
import os
import re
import unicodedata
from datetime import date

import pandas as pd

from app.services.utils import (
    agregar_cero,
    formatear_porcentaje,
    leer_archivo,
    leer_resolucion_txt,
)

COMUNAS_RESTRINGIDAS = {"colina", "las condes", "vitacura", "lo barnechea"}

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

_RE_YYYYMM = re.compile(r"(20\d{2})(0[1-9]|1[0-2])")


# ─────────────────────────────────────────────────────────────
# Helpers de columnas (tolerante a acentos/mayúsculas/espacios)
# ─────────────────────────────────────────────────────────────

def _normalizar_col(nombre: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(nombre))
    sin_acentos = "".join(c for c in nfkd if not unicodedata.combining(c))
    return sin_acentos.strip().lower().replace(" ", "").replace("_", "")


_PREFIJO_XLS = "XLS__"


def _col(df: pd.DataFrame, *candidatos: str, prefer_prefix: str | None = None, exclude_prefix: str | None = None) -> str | None:
    columnas = df.columns
    if exclude_prefix:
        columnas = [c for c in columnas if not c.startswith(exclude_prefix)]
    if prefer_prefix:
        columnas = [c for c in columnas if c.startswith(prefer_prefix)]
    mapa = {}
    for c in columnas:
        clave = c[len(prefer_prefix):] if prefer_prefix else c
        mapa[_normalizar_col(clave)] = c
    for cand in candidatos:
        real = mapa.get(_normalizar_col(cand))
        if real:
            return real
    return None


def _col_txt(df: pd.DataFrame, *candidatos: str) -> str | None:
    """Busca una columna proveniente del TXT de resoluciones (ignora las del Excel cruzado)."""
    return _col(df, *candidatos, exclude_prefix=_PREFIJO_XLS)


def _col_xls(df: pd.DataFrame, *candidatos: str) -> str | None:
    """
    Busca una columna proveniente del Excel mensual cruzado por RUT.
    Necesario porque el TXT y el Excel mensual a veces comparten nombre de
    columna (ej. REFI: ambos traen "Tasa"/"DCTO_TASA" con significados
    distintos) — sin este prefijo explícito, el cruce podía traer por
    accidente el valor del TXT en vez del Excel (o viceversa).
    """
    return _col(df, *candidatos, prefer_prefix=_PREFIJO_XLS)


def _serie(df: pd.DataFrame, col: str | None) -> pd.Series:
    if col and col in df.columns:
        return df[col].fillna("")
    return pd.Series([""] * len(df), index=df.index)


def _mes_desde_nombre(nombre_archivo: str) -> tuple[str, str]:
    """Extrae (aaaamm, nombre_mes) del nombre del archivo mensual; si no
    encuentra el patrón AAAAMM, usa el mes actual."""
    m = _RE_YYYYMM.search(nombre_archivo or "")
    if m:
        aaaa, mm = m.group(1), m.group(2)
        return f"{aaaa}{mm}", MESES_ES[int(mm)]
    hoy = date.today()
    return hoy.strftime("%Y%m"), MESES_ES[hoy.month]


# ─────────────────────────────────────────────────────────────
# Cruce por RUT
# ─────────────────────────────────────────────────────────────

def _limpiar_rut(serie: pd.Series) -> pd.Series:
    return serie.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)


def _cruzar_por_rut(df_txt: pd.DataFrame, col_rut_txt: str, df_excel: pd.DataFrame, col_rut_excel: str) -> pd.DataFrame:
    """
    Cruce izquierdo por RUT. Todas las columnas del Excel se renombran con
    el prefijo XLS__ (ver `_col_xls`) para que nunca se confundan con una
    columna del TXT que tenga el mismo nombre (ej. REFI: "Tasa"/"DCTO_TASA").
    Agrega una columna booleana "_cruce": True si el RUT existe en el Excel.
    """
    df_txt = df_txt.copy()
    df_excel = df_excel.copy()
    df_txt["_RUT"] = _limpiar_rut(df_txt[col_rut_txt])
    df_excel["_RUT"] = _limpiar_rut(df_excel[col_rut_excel])
    df_excel = df_excel.rename(columns={c: f"{_PREFIJO_XLS}{c}" for c in df_excel.columns if c != "_RUT"})
    df_merge = df_txt.merge(df_excel, on="_RUT", how="left", indicator="_indicador_cruce")
    df_merge["_cruce"] = df_merge["_indicador_cruce"] == "both"
    return df_merge.drop(columns=["_RUT", "_indicador_cruce"])


def _vacio(valor) -> bool:
    s = str(valor).strip().lower()
    return s in ("", "nan", "none")


# ─────────────────────────────────────────────────────────────
# Construcción del Update
# ─────────────────────────────────────────────────────────────

def _construir_update_pl(df: pd.DataFrame, mes_nombre: str, fecha_hoy: str) -> pd.DataFrame:
    n = len(df)
    id_col = _col_txt(df, "Id contacto")
    pie_col = _col_xls(df, "CON_SIN_PIE", "PIE")
    marca_col = _col_xls(df, "Marca_propension")
    marca_vals = _serie(df, marca_col)

    return pd.DataFrame({
        "Identificador Contacto": _serie(df, id_col),
        "Teléfono 2":             _serie(df, _col_xls(df, "TELEFONO2")).apply(agregar_cero),
        "Teléfono 3":             _serie(df, _col_xls(df, "TELEFONO3")).apply(agregar_cero),
        "Marca":                  marca_vals,
        "Telefono 1":             _serie(df, _col_xls(df, "TELEFONO1")).apply(agregar_cero),
        "PIE":                    _serie(df, pie_col),
        "Orden Discado":          [99999] * n,
        "FECHAVCTO":              _serie(df, _col_xls(df, "FECHAVCTO")),
        "TipoBase":               ["PL Normal"] * n,
        "PRODUCTO":               _serie(df, _col_xls(df, "PRODUCTO")),
        "Tasa":                   _serie(df, _col_xls(df, "tasa618")),
        "Novedad":                _serie(df, _col_xls(df, "NOVEDAD")),
        "BDD":                    [f"Base PL {mes_nombre}"] * n,
        "FechaCarga":             [fecha_hoy] * n,
        "Descuento Tasa":         _serie(df, _col_xls(df, "Descuento")),
        "Propension":             marca_vals,
        "MarcaEstrategia":        _serie(df, _col_xls(df, "MARCA_ESTRATEGIA")),
        "AV":                     _serie(df, _col_xls(df, "AV")),
        "SAV":                    _serie(df, _col_xls(df, "SAV")),
        "VencimentoTarjeta":      _serie(df, _col_xls(df, "Vencimiento Tarjeta")),
        "Propension_Mora":        _serie(df, _col_xls(df, "Propension_Mora")),
        "FECHA_INICIO":           _serie(df, _col_xls(df, "FECHA_INICIO", "Fecha Inicio", "Fecha_inicio")),
        "FECHA_TERMINO":          _serie(df, _col_xls(df, "FECHA_TERMINO", "Fecha Termino", "Fecha_termino", "Fecha_final")),
    })


def _construir_update_refi(df: pd.DataFrame, mes_nombre: str, fecha_hoy: str) -> pd.DataFrame:
    n = len(df)
    tasa_col = _col_xls(df, "TASA")
    dcto_col = _col_xls(df, "DCTO_TASA")

    return pd.DataFrame({
        "Identificador Contacto": _serie(df, _col_txt(df, "Id contacto")),
        "Teléfono 2":             _serie(df, _col_xls(df, "TELEFONO2")).apply(agregar_cero),
        "Teléfono 3":             _serie(df, _col_xls(df, "TELEFONO3")).apply(agregar_cero),
        "Telefono 1":             _serie(df, _col_xls(df, "TELEFONO1")).apply(agregar_cero),
        "Orden Discado":          [99999] * n,
        "FECHAVCTO":              _serie(df, _col_xls(df, "VENCIMIENTO")),
        "TipoBase":               ["RN Normal"] * n,
        "Tasa":                   _serie(df, tasa_col).apply(formatear_porcentaje),
        "BDD":                    [f"Base RN {mes_nombre}"] * n,
        "Fecha Carga":            [fecha_hoy] * n,
        "Propension":             _serie(df, _col_xls(df, "PROPENSION")),
        "DCTO_TASA":              _serie(df, dcto_col).apply(formatear_porcentaje),
        "Fecha_inicio":           _serie(df, _col_xls(df, "Fecha_inicio")),
        "Fecha_final":            _serie(df, _col_xls(df, "Fecha_final")),
        "PROPENSION_MORA":        _serie(df, _col_xls(df, "PROPENSION_MORA")),
    })


# ─────────────────────────────────────────────────────────────
# DetalleCarga (libro de revisión, 2 hojas)
# ─────────────────────────────────────────────────────────────

def _guardar_detalle_carga(df: pd.DataFrame, mask_restringida: pd.Series, path: str) -> str:
    df_export = df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")
    base_cargada = df_export[~mask_restringida]
    no_cargados = df_export[mask_restringida]

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        base_cargada.to_excel(writer, sheet_name="Base Cargada", index=False)
        no_cargados.to_excel(writer, sheet_name="No Cargados Comunas", index=False)
    return path


# ─────────────────────────────────────────────────────────────
# Export del Update a TXT (BCP, sin encabezado) — reemplaza el .xls
# de antes. El orden y los nombres reales de columna acá DEBEN
# coincidir exactamente con FORMATO_ACTUALIZAR_{PL,REFI}.xml y con el
# CREATE TABLE #TEMP de neotel_ds_actualizar_datos_{pl,refi}.sql — los
# 3 son parte del mismo contrato, si se cambia uno hay que cambiar los
# otros dos. (columna_real, columna_en_df_update)
# ─────────────────────────────────────────────────────────────

_CAMPOS_TXT_ACTUALIZAR = {
    "PL": [
        ("IDINTERNO", "Identificador Contacto"),
        ("telTelefono2", "Teléfono 2"),
        ("telTelefono3", "Teléfono 3"),
        ("telTelefono1", "Telefono 1"),
        ("txtTipoPropension", "Marca"),
        ("txtPie", "PIE"),
        ("intOrdenDiscado", "Orden Discado"),
        ("intFECHAVCTO", "FECHAVCTO"),
        ("txtTipoBase", "TipoBase"),
        ("txtPRODUCTO", "PRODUCTO"),
        ("txtTasa", "Tasa"),
        ("txtNovedad", "Novedad"),
        ("txtBDD", "BDD"),
        ("txtFechaCarga", "FechaCarga"),
        ("txtDescuentoTasa", "Descuento Tasa"),
        ("txtPropension", "Propension"),
        ("txtMarcaEstrategia", "MarcaEstrategia"),
        ("txtAV", "AV"),
        ("txtSAV", "SAV"),
        ("txtVencimentoTarjeta", "VencimentoTarjeta"),
        ("txtPropensionMora", "Propension_Mora"),
        ("txtFECHAINICIO", "FECHA_INICIO"),
        ("txtFECHATERMINO", "FECHA_TERMINO"),
    ],
    "REFI": [
        ("IDINTERNO", "Identificador Contacto"),
        ("telTelefono2", "Teléfono 2"),
        ("telTelefono3", "Teléfono 3"),
        ("telTelefono1", "Telefono 1"),
        ("intOrdenDiscado", "Orden Discado"),
        ("intFECHAVCTO", "FECHAVCTO"),
        ("txtTipoBase", "TipoBase"),
        ("txtTasa", "Tasa"),
        ("txtBDD", "BDD"),
        ("txtFechaCarga", "Fecha Carga"),
        ("txtPropension", "Propension"),
        ("txtDCTOTASA", "DCTO_TASA"),
        ("txtFechainicio", "Fecha_inicio"),
        ("txtFechafinal", "Fecha_final"),
        ("txtPROPENSIONMORA", "PROPENSION_MORA"),
    ],
}


def _exportar_update_txt(df_update: pd.DataFrame, tipo: str, path: str) -> str | None:
    """Exporta `df_update` a TXT pipe-delimited SIN encabezado, en el
    orden fijo de columnas reales que espera el nuevo Datasource
    "Actualizar Datos" (ver comentario arriba)."""
    if df_update is None or len(df_update) == 0:
        return None

    campos = _CAMPOS_TXT_ACTUALIZAR[tipo]
    n = len(df_update)
    datos = {}
    for col_real, col_df in campos:
        serie = df_update[col_df] if col_df in df_update.columns else [""] * n
        limpios = []
        for v in serie:
            if v is None or (isinstance(v, float) and v != v):
                limpios.append("")
                continue
            texto = str(v).strip()
            if texto in ("nan", "None"):
                texto = ""
            # El '|' es el delimitador del archivo: nunca puede ir dentro de un valor
            texto = texto.replace("|", " ").replace("\r", "").replace("\n", "")
            limpios.append(texto)
        datos[col_real] = limpios

    base, _ext = os.path.splitext(path)
    path = base + ".txt"
    df_salida = pd.DataFrame(datos, columns=[c for c, _ in campos])
    df_salida.to_csv(path, sep="|", index=False, header=False, encoding="latin1", lineterminator="\n")
    print(f"✅ Update TXT generado: {os.path.basename(path)}")
    return path


def _guardar_eliminar_txt(ids: list[str], path: str) -> str:
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(str(i).strip() for i in ids if not _vacio(i)))
    return path


# ─────────────────────────────────────────────────────────────
# Control de archivos ya usados — evita reprocesar/reaplicar por
# accidente el mismo TXT de Resultante o el mismo Excel mensual dos
# veces (ambos "encontrar_..._reciente" solo miran "el más nuevo", sin
# registrar cuál fue el último que ya se usó).
# ─────────────────────────────────────────────────────────────

def _identidad_archivo(ruta: str, mtime: float) -> str:
    return f"{ruta}|{int(mtime)}"


def identidad_desde_contenido(contenido: bytes) -> tuple[str, float]:
    """
    Identidad basada en el hash SHA256 del contenido — para archivos
    subidos directamente desde la PC (sin ruta en el FTP, así que no hay
    mtime que usar). Se le da la misma forma (str, float) que
    _identidad_archivo espera, con mtime fijo en 0 (no se usa acá, es
    solo para que el par completo — hash+"0" — sea la clave de igualdad).
    """
    import hashlib
    return (f"hash:{hashlib.sha256(contenido).hexdigest()}", 0.0)


def _verificar_archivo_nuevo(tipo: str, clave: str, identidad: tuple[str, float] | None) -> None:
    """
    Bloquea si `identidad` (ruta o hash, mtime) ya se procesó antes para
    `tipo` — `clave` es "TXT" o "EXCEL". `identidad` es None solo si no
    se pudo determinar ninguna identidad (ni ruta+mtime del FTP ni hash
    del contenido) — no debería pasar en la práctica.
    """
    if not identidad:
        return
    ruta, mtime = identidad
    from app.core.postgres import get_config_valor
    anterior = get_config_valor(f"CARGA_MENSUAL_{tipo}_ULTIMO_{clave}")
    if anterior and anterior == _identidad_archivo(ruta, mtime):
        raise RuntimeError(
            f"Este {clave.lower()} ya se procesó antes para {tipo} ({ruta}) — "
            f"no se puede volver a procesar el mismo archivo."
        )


def _marcar_archivo_usado(tipo: str, clave: str, identidad: tuple[str, float] | None) -> None:
    if not identidad:
        return
    ruta, mtime = identidad
    from app.core.postgres import set_config_global
    set_config_global({f"CARGA_MENSUAL_{tipo}_ULTIMO_{clave}": _identidad_archivo(ruta, mtime)})


# ─────────────────────────────────────────────────────────────
# Entradas principales
# ─────────────────────────────────────────────────────────────

def _procesar_carga(
    tipo: str,
    txt_bytes: bytes,
    txt_nombre: str,
    excel_bytes: bytes,
    excel_nombre: str,
    output_dir: str = "/tmp",
    progress_cb=None,
    txt_identidad: tuple[str, float] | None = None,
    excel_identidad: tuple[str, float] | None = None,
) -> dict:
    def emit(step):
        if progress_cb:
            progress_cb(step)

    tipo = tipo.upper()
    if tipo not in ("PL", "REFI"):
        raise ValueError("tipo debe ser 'PL' o 'REFI'")

    _verificar_archivo_nuevo(tipo, "TXT", txt_identidad)
    _verificar_archivo_nuevo(tipo, "EXCEL", excel_identidad)

    aaaamm, mes_nombre = _mes_desde_nombre(excel_nombre)
    fecha_hoy = date.today().strftime("%d-%m-%Y")

    emit("Leyendo TXT de resoluciones")
    df_txt = leer_resolucion_txt(txt_bytes)

    emit("Leyendo Excel mensual")
    df_excel = leer_archivo(excel_bytes, excel_nombre)
    df_excel.columns = df_excel.columns.str.strip()

    id_col = _col_txt(df_txt, "Id contacto")
    rut_txt_col = _col_txt(df_txt, "RUT")
    comuna_col = _col_txt(df_txt, "Comuna_Particular")

    total_entrada = len(df_txt)
    print(f"[CargaMensual-{tipo}] Entrada TXT: {total_entrada} filas")

    if tipo == "PL":
        # El filtro NO es sobre el valor propio de "PIE" en el TXT: es el
        # mismo mecanismo que REFI — cruzar por RUT contra el Excel mensual
        # y descartar los que dan "#N/D" (no cruzan) o cuyo CON_SIN_PIE
        # viene vacío. El "PIE" del Update sale del Excel (CON_SIN_PIE), no
        # del TXT.
        rut_excel_col = _col(df_excel, "CTARUT", "RUT")

        df_merge_completo = _cruzar_por_rut(df_txt, rut_txt_col, df_excel, rut_excel_col)
        pie_col_xls = _col_xls(df_merge_completo, "CON_SIN_PIE", "PIE")
        mask_ok = ~df_merge_completo[pie_col_xls].apply(_vacio) if pie_col_xls else pd.Series([False] * len(df_merge_completo))

        df_merge = df_merge_completo[mask_ok].reset_index(drop=True)
        df_elim1 = df_merge_completo[~mask_ok].reset_index(drop=True)
        sin_cruce = len(df_elim1)
        no_encontrados = int((~df_merge_completo["_cruce"]).sum())
        print(
            f"[CargaMensual-PL] Cruce por RUT vs Excel mensual ({rut_excel_col}) trayendo {pie_col_xls}: "
            f"{len(df_merge)} quedan, {sin_cruce} descontados "
            f"({no_encontrados} por RUT no encontrado, {sin_cruce - no_encontrados} por {pie_col_xls} vacío)"
        )
        emit(f"Cruce por RUT: {len(df_merge)} ok / {sin_cruce} sin cruce")
    else:  # REFI
        rut_excel_col = _col(df_excel, "RUT_TARJETA", "RUT")

        df_merge_completo = _cruzar_por_rut(df_txt, rut_txt_col, df_excel, rut_excel_col)
        div_col = _col_xls(df_merge_completo, "DIV", "DV", "Digito")
        mask_ok = ~df_merge_completo[div_col].apply(_vacio) if div_col else pd.Series([False] * len(df_merge_completo))

        df_merge = df_merge_completo[mask_ok].reset_index(drop=True)
        df_elim1 = df_merge_completo[~mask_ok].reset_index(drop=True)
        sin_cruce = len(df_elim1)
        no_encontrados = int((~df_merge_completo["_cruce"]).sum())
        print(
            f"[CargaMensual-REFI] Cruce por RUT vs Excel mensual ({rut_excel_col}) trayendo {div_col}: "
            f"{len(df_merge)} quedan, {sin_cruce} descontados "
            f"({no_encontrados} por RUT no encontrado, {sin_cruce - no_encontrados} por {div_col} vacío)"
        )
        emit(f"Cruce por RUT: {len(df_merge)} ok / {sin_cruce} sin cruce")

    ids_elim1 = df_elim1[id_col].astype(str).str.strip().tolist() if id_col else []
    print(f"[CargaMensual-{tipo}] Total a eliminar por filtro/cruce: {len(ids_elim1)}")

    # ── Comunas restringidas: se calculan ANTES del Update, para que esos
    #    registros NO se carguen (no basta con borrarlos después vía IDINTERNO) ──
    if comuna_col and comuna_col in df_merge.columns:
        mask_restringida = df_merge[comuna_col].astype(str).str.strip().str.lower().isin(COMUNAS_RESTRINGIDAS)
    else:
        mask_restringida = pd.Series([False] * len(df_merge))

    ids_no_cargados = df_merge.loc[mask_restringida, id_col].astype(str).str.strip().tolist() if id_col else []
    print(f"[CargaMensual-{tipo}] Comunas restringidas ({', '.join(sorted(COMUNAS_RESTRINGIDAS))}): {len(ids_no_cargados)} descontados")

    df_para_update = df_merge[~mask_restringida].reset_index(drop=True)

    # ── Construir Update (sin las comunas restringidas) ──
    emit("Construyendo Update")
    if tipo == "PL":
        df_update = _construir_update_pl(df_para_update, mes_nombre, fecha_hoy)
    else:
        df_update = _construir_update_refi(df_para_update, mes_nombre, fecha_hoy)

    path_update = os.path.join(output_dir, f"Update{tipo}{aaaamm}.txt")
    emit("Exportando Update (.txt)")
    print(f"[CargaMensual-{tipo}] Construyendo Update: {len(df_update)} filas")
    ruta_update = _exportar_update_txt(df_update, tipo, path_update)
    rutas_update = [ruta_update] if ruta_update else []

    emit("Generando libro DetalleCarga")
    path_detalle = os.path.join(output_dir, f"DetalleCarga{tipo}{aaaamm}.xlsx")
    _guardar_detalle_carga(df_merge, mask_restringida, path_detalle)

    emit("Generando eliminar.txt")
    path_eliminar = os.path.join(output_dir, f"eliminar_{tipo}{aaaamm}.txt")
    ids_eliminar = ids_elim1 + ids_no_cargados
    _guardar_eliminar_txt(ids_eliminar, path_eliminar)
    print(
        f"[CargaMensual-{tipo}] RESUMEN → entrada: {total_entrada}, "
        f"carga final: {len(df_update)}, "
        f"eliminar.txt total: {len(ids_eliminar)} "
        f"(filtro/cruce: {len(ids_elim1)} + comuna: {len(ids_no_cargados)})"
    )

    _marcar_archivo_usado(tipo, "TXT", txt_identidad)
    _marcar_archivo_usado(tipo, "EXCEL", excel_identidad)

    return {
        "archivo_detalle":       path_detalle,
        "archivos_update":       rutas_update,
        "archivo_eliminar":      path_eliminar,
        "total_entrada":         total_entrada,
        "total_sin_cruce":       sin_cruce,
        "total_eliminados":      len(ids_eliminar),
        "total_no_cargados_comunas": len(ids_no_cargados),
        "total_carga":           len(df_update),
        "aaaamm":                aaaamm,
        "_nombre_txt":           txt_nombre,
        "_nombre_excel":         excel_nombre,
    }


def procesar_carga_pl(
    txt_bytes: bytes,
    txt_nombre: str,
    excel_bytes: bytes,
    excel_nombre: str,
    output_dir: str = "/tmp",
    progress_cb=None,
    txt_identidad: tuple[str, float] | None = None,
    excel_identidad: tuple[str, float] | None = None,
) -> dict:
    return _procesar_carga(
        "PL", txt_bytes, txt_nombre, excel_bytes, excel_nombre, output_dir, progress_cb,
        txt_identidad, excel_identidad,
    )


def procesar_carga_refi(
    txt_bytes: bytes,
    txt_nombre: str,
    excel_bytes: bytes,
    excel_nombre: str,
    output_dir: str = "/tmp",
    progress_cb=None,
    txt_identidad: tuple[str, float] | None = None,
    excel_identidad: tuple[str, float] | None = None,
) -> dict:
    return _procesar_carga(
        "REFI", txt_bytes, txt_nombre, excel_bytes, excel_nombre, output_dir, progress_cb,
        txt_identidad, excel_identidad,
    )


# ─────────────────────────────────────────────────────────────
# Aplicar en Neotel: sube el/los Update(s) y el eliminar.txt (ya
# generados por procesar_carga_pl/refi arriba) al FTP y dispara la
# Tarea "Actualizar Datos + Eliminar" (Datasources 27 y 1036 — ver
# app.core.neotel_ws) que hasta ahora Jorge hacía a mano en Neotel.
# Queda separado de _procesar_carga a propósito: ese paso solo genera
# archivos para revisar (DetalleCarga); este otro sí escribe/borra en
# vivo, así que se dispara aparte, con confirmación explícita.
# ─────────────────────────────────────────────────────────────

_CLIENTE_COD_APLICAR = {"PL": "0001", "REFI": "0289"}
_KEY_ID_TAREA_ACTUALIZAR_DATOS = "ID_TAREA_ACTUALIZAR_DATOS"


def aplicar_en_neotel(
    tipo: str,
    iddatabase: int,
    archivos_update: list[str],
    archivo_eliminar: str,
    usuario: str,
    progress_cb=None,
) -> dict:
    """
    Sube el Update (TXT, ver _exportar_update_txt) y el eliminar.txt al
    FTP de Neotel (/UPLOAD/Update{tipo}/) y dispara la Tarea
    "Actualizar Datos + Eliminar" con 4 parámetros: ClienteCod, BASE,
    ARCHIVO_ACTUALIZAR, ARCHIVO_ELIMINAR — el nuevo Datasource
    "Actualizar Datos" lee el TXT vía OPEN_TXT_FORMAT (BCP nativo, no
    depende de ACE OLEDB/DCOM como el OPEN_XLS_3 original, que fallaba
    al dispararse desde el motor de Tareas).

    `archivos_update` sigue siendo una lista por compatibilidad con
    quien la llama, pero ya no puede tener más de un elemento — el TXT
    no tiene el límite de 65.536 filas del .xls, así que no hace falta
    partirlo.
    """
    def emit(step):
        if progress_cb:
            progress_cb(step)

    tipo = tipo.upper()
    if tipo not in ("PL", "REFI"):
        raise ValueError("tipo debe ser 'PL' o 'REFI'")
    if len(archivos_update) > 1:
        raise ValueError("archivos_update no debería tener más de 1 elemento (TXT, sin límite de filas)")

    from app.core.postgres import get_config_valor, registrar_auditoria
    from app.core.ftp_neotel17 import subir_archivo_ftp17
    from app.core.neotel_ws import ejecutar_tarea_con_parametros

    id_tarea = get_config_valor(_KEY_ID_TAREA_ACTUALIZAR_DATOS)
    if not id_tarea:
        raise RuntimeError(f"Falta configurar {_KEY_ID_TAREA_ACTUALIZAR_DATOS} en config_global")

    carpeta = f"Update{tipo}"

    emit("Subiendo eliminar.txt")
    nombre_eliminar = os.path.basename(archivo_eliminar)
    subir_archivo_ftp17(archivo_eliminar, f"/UPLOAD/{carpeta}/{nombre_eliminar}")
    archivo_eliminar_windows = f"D:\\NEOTEL\\FTP\\UPLOAD\\{carpeta}\\{nombre_eliminar}"

    archivo_actualizar_windows = ""
    if archivos_update:
        nombre_update = os.path.basename(archivos_update[0])
        emit(f"Subiendo {nombre_update}")
        subir_archivo_ftp17(archivos_update[0], f"/UPLOAD/{carpeta}/{nombre_update}")
        archivo_actualizar_windows = f"D:\\NEOTEL\\FTP\\UPLOAD\\{carpeta}\\{nombre_update}"

    emit("Disparando actualización + eliminación en Neotel")
    parametros = [
        _CLIENTE_COD_APLICAR[tipo], str(iddatabase), archivo_actualizar_windows, archivo_eliminar_windows,
    ]
    resultado = ejecutar_tarea_con_parametros(int(id_tarea), parametros)
    if resultado is None or resultado.startswith("ERROR"):
        raise RuntimeError(resultado or "No se pudo disparar la Tarea")

    registrar_auditoria(
        usuario, f"APLICAR_CARGA_MENSUAL_{tipo}",
        f"IDDATABASE={iddatabase} archivo_update={os.path.basename(archivos_update[0]) if archivos_update else '(ninguno)'} "
        f"archivo_eliminar={nombre_eliminar}",
    )

    emit("Listo")
    return {"resultados": [resultado]}