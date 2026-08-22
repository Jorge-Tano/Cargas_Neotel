"""
carga_mensual.py
=================
Automatiza la parte de Excel de la carga mensual normal (no-Leakage) de
Pago Liviano (PL) y Refinanciamiento (REFI/RN):

  1. Cruza el TXT de resoluciones (FTP Neotel17, /DOWNLOAD/Resultante_PL)
     contra el Excel mensual de la campaña (FTP principal,
     /archivos/{año}/OP/{mes}) por RUT.
  2. Separa los registros que no aplican (van a eliminar.txt).
  3. Arma la(s) plantilla(s) Update (.xls, tope 65.536 filas → se parte).
  4. Arma el libro DetalleCarga con hojas "Base Cargada" / "No Cargados Comunas".

Todo lo anterior a esto (crear/asociar la base en ECRM, ejecutar la tarea de
depósito, importar el Update, borrar por IDINTERNO) sigue siendo manual.
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
    exportar_excel_particionado,
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


def _guardar_eliminar_txt(ids: list[str], path: str) -> str:
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(str(i).strip() for i in ids if not _vacio(i)))
    return path


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
) -> dict:
    def emit(step):
        if progress_cb:
            progress_cb(step)

    tipo = tipo.upper()
    if tipo not in ("PL", "REFI"):
        raise ValueError("tipo debe ser 'PL' o 'REFI'")

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

    path_update_base = os.path.join(output_dir, f"Update{tipo}{aaaamm}.xls")
    emit("Exportando Update (.xls)")
    print(f"[CargaMensual-{tipo}] Construyendo Update: {len(df_update)} filas")
    rutas_update = exportar_excel_particionado(df_update, path_update_base, sheet_name="Sheet1")
    print(f"[CargaMensual-{tipo}] Update generado en {len(rutas_update)} archivo(s): {[os.path.basename(r) for r in rutas_update]}")

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
) -> dict:
    return _procesar_carga("PL", txt_bytes, txt_nombre, excel_bytes, excel_nombre, output_dir, progress_cb)


def procesar_carga_refi(
    txt_bytes: bytes,
    txt_nombre: str,
    excel_bytes: bytes,
    excel_nombre: str,
    output_dir: str = "/tmp",
    progress_cb=None,
) -> dict:
    return _procesar_carga("REFI", txt_bytes, txt_nombre, excel_bytes, excel_nombre, output_dir, progress_cb)