"""
Caso: Llamadas Perdidas
─────────────────────────────────────────────
Entrada: perdidas_{fecha}.xlsx
  - Columna clave: /Teléfono

Proceso:
  1. Leer archivo
  2. Extraer columna de teléfono (/Teléfono)
  3. Aplicar regla del 0 (agregar_cero)
  4. Separar registros sin teléfono válido (agregar_cero devuelve "00"
     cuando el dato viene vacío/inválido) a un archivo aparte, para que
     NO queden mezclados en la carga.
  5. Generar archivo CargaLlamadasPerdidas{fecha}.xlsx

Salida:
  - CargaLlamadasPerdidas{fecha}.xlsx      (solo teléfonos válidos)
  - SinTelefonoLlamadasPerdidas{fecha}.xlsx (revisión manual)
  Columnas: Telefono1, FechaCarga, FechaLlamado

Subida a Neotel (solo para PERDIDAS_USUARIO_AUTORIZADO, ver .env):
  Genera además un TXT con el mismo layout completo del Excel de Carga
  (COLUMNAS_SALIDA — confirmado contra un archivo real de Contactos) y
  lo sube a /UPLOAD/cargas_manual, dispara ExecuteTask00 y confirma vía
  LOTES — igual que los demás casos (ver
  app.core.neotel_ws/confirmacion_carga). Solo Telefono1/FechaCarga/
  FechaLlamado vienen poblados hoy; el resto de columnas viaja vacío.
  Destino: ECRM_0271, IDDATABASE=21 (fijo, no rota como SAV/AV/REFI/PL).

  IMPORTANTE: a diferencia de los demás casos, Neotel todavía NO tiene
  un script de import ni una tarea de ExecuteTask00 para este caso —
  hay que crearlos primero (ver conversación/PR que agregó esto). Hasta
  entonces, el TXT se sube pero nadie lo importa, así que la
  confirmación siempre va a salir "no confirmada" — es esperado, no un bug.

  Para cualquier otro usuario (no PERDIDAS_USUARIO_AUTORIZADO), el
  proceso termina en el Excel de siempre, sin tocar Neotel.
"""

import pandas as pd
import io
from datetime import date, datetime
from app.core.postgres import registrar_log
from app.services.utils import agregar_cero, exportar_excel, exportar_multi_destino, leer_archivo, exportar_txt_carga, extraer_horario_archivo


COLUMNAS_SALIDA = [
    "Rut", "Digito", "Nombre Cliente", "Apellido Paterno", "Apellido Materno",
    "DISPONIBLE_SA", "DetalleOferta", "Telefono1", "Telefono2", "Telefono3",
    "Telefono4", "Telefono5", "Telefono6", "Producto", "FechaCarga",
    "FechaLlamado", "Estado", "OrdenDiscado"
]

# Layout del TXT que se sube a Neotel (/UPLOAD/cargas_manual) — mismo
# layout real que el Excel de Carga (COLUMNAS_SALIDA), confirmado contra
# un archivo de referencia real de Contactos. A diferencia de los demás
# casos, este script de import todavía NO existe en Neotel (ver
# docstring del módulo) — solo Telefono1/FechaCarga/FechaLlamado vienen
# poblados hoy (el resto queda vacío), pero se sube el layout completo
# por si a futuro se completan más columnas desde el origen.
COLUMNAS_TXT_PERDIDAS = COLUMNAS_SALIDA


def procesar_llamadas_perdidas(
    archivo_bytes: bytes,
    nombre_archivo: str,
    output_dirs: dict = None,
    progress_cb=None,
    usuario: str = "",
    procesar_neotel: bool = True,
) -> dict:
    def emit(step):
        if progress_cb:
            progress_cb(step)

    output_dirs = output_dirs or {}
    hoy = date.today().strftime("%Y%m%d")
    fecha_carga = date.today().strftime("%d/%m/%Y")

    # 1. Leer archivo
    emit("Leyendo archivo")
    df = leer_archivo(archivo_bytes, nombre_archivo)
    df.columns = df.columns.str.strip()
    total_entrada = len(df)

    # 2. Identificar columna de teléfono
    emit("Identificando columnas")
    col_telefono = None
    for col in df.columns:
        if "tel" in col.lower() or "fono" in col.lower():
            col_telefono = col
            break

    if col_telefono is None:
        raise ValueError("No se encontró columna de teléfono en el archivo de entrada.")

    # 3. Construir DataFrame de salida
    emit("Construyendo archivo de carga")
    df_salida = pd.DataFrame(columns=COLUMNAS_SALIDA)
    df_salida["Telefono1"] = df[col_telefono].apply(agregar_cero)
    df_salida["FechaCarga"] = fecha_carga
    if "Inicio" in df.columns:
        df_salida["FechaLlamado"] = df["Inicio"]
    df_salida = df_salida.fillna("")

    # 3b. Separar registros con teléfono inválido/vacío (agregar_cero
    # devuelve "00" cuando no hay un número real). No se cargan: se
    # dejan en un archivo aparte para revisión manual.
    emit("Separando registros con teléfono inválido")
    mask_sin_telefono = df_salida["Telefono1"].astype(str).str.strip() == "00"
    df_sin_telefono = df_salida[mask_sin_telefono].reset_index(drop=True)
    df_salida       = df_salida[~mask_sin_telefono].reset_index(drop=True)

    # 4. Exportar: Carga va a compartida y a local; Sin Teléfono solo a compartida
    emit("Generando archivo Excel")
    nombre_salida       = f"CargaLlamadasPerdidas{hoy}.xls"
    nombre_sin_telefono = f"SinTelefonoLlamadasPerdidas{hoy}.xls"
    tareas = [
        (df_salida,       nombre_salida,       "Contactos", False, "carga"),
        (df_sin_telefono, nombre_sin_telefono, "Contactos", False, "sin_telefono"),
    ]
    paths = exportar_multi_destino(tareas, output_dirs, claves_local={"carga"})
    path_salida       = paths["carga"]
    path_sin_telefono = paths["sin_telefono"]

    # 5. Registrar log (los "sin teléfono" se registran como bloqueados
    # para que queden visibles en el log de auditoría)
    registrar_log(
        tipo_caso="PERDIDAS",
        total_entrada=total_entrada,
        total_repetidos=0,
        total_bloqueados=len(df_sin_telefono),
        total_carga=len(df_salida),
        archivo_origen=nombre_archivo,
        usuario=usuario,
    )

    # 6. Subir a Neotel — SOLO si procesar_neotel viene en True. Quién puede
    #    pedir eso ya se decidió antes de llegar acá (main._neotel_efectivo,
    #    según permisos por usuario) — acá no se vuelve a chequear usuario.
    #
    #    OJO: Neotel todavía NO tiene una tarea de import para este caso
    #    (a diferencia de SAV/AV/REFI/PL/CARRITO/MKT) — falta que alguien
    #    cree en Neotel el script de import (para /UPLOAD/cargas_manual,
    #    ECRM_0271/IDDATABASE=21) y la tarea de ExecuteTask00 correspondiente.
    #    Sin eso, el TXT se sube igual pero nadie lo importa todavía, así
    #    que la confirmación de más abajo va a salir "no confirmada" hasta
    #    que esa tarea exista — es el comportamiento correcto mientras
    #    tanto, no un bug.
    if len(df_salida) > 0 and procesar_neotel:
        emit("Generando archivo de carga en TXT para Neotel")
        carpeta_txt = output_dirs.get("compartida") or output_dirs.get("local") or "/tmp"
        horario_txt = extraer_horario_archivo(nombre_archivo or "") or datetime.now().strftime("%H%M")
        nombre_carga_txt = f"SalidaLlamadasPerdidas{hoy}{horario_txt}.txt"
        path_carga_txt = exportar_txt_carga(df_salida, f"{carpeta_txt}/{nombre_carga_txt}", COLUMNAS_TXT_PERDIDAS)

        carga_forzada = False
        hora_disparo = None
        if path_carga_txt:
            emit("Subiendo TXT de carga por FTP a Neotel")
            try:
                from app.core.ftp_neotel17 import subir_archivo_carga_txt
                subir_archivo_carga_txt(path_carga_txt, tipo="PERDIDAS")
            except Exception as e:
                print(f"⚠️  Error subiendo TXT por FTP: {e}")

            emit("Disparando import inmediato en Neotel")
            try:
                from app.core.sqlserver import obtener_hora_neotel
                from app.core.neotel_ws import ejecutar_tarea
                hora_disparo = obtener_hora_neotel()
                carga_forzada = ejecutar_tarea("PERDIDAS") is not None
            except Exception as e:
                print(f"⚠️  Error disparando import en Neotel: {e}")

        try:
            from app.core.confirmacion_carga import confirmar_carga_en_segundo_plano
            confirmar_carga_en_segundo_plano(
                caso="PERDIDAS",
                valores=df_salida["Telefono1"].astype(str).tolist(),
                columna="TELTELEFONO1",
                archivo_origen=nombre_archivo,
                usuario=usuario,
                carga_forzada=carga_forzada,
                hora_disparo=hora_disparo,
            )
        except Exception as e:
            print(f"⚠️  Error iniciando confirmación de carga PERDIDAS en Neotel: {e}")

    return {
        "archivo_carga":        path_salida,
        "archivo_sin_telefono": path_sin_telefono,
        "total_entrada":        total_entrada,
        "total_carga":          len(df_salida),
        "total_sin_telefono":   len(df_sin_telefono),
        "fecha":                hoy,
    }