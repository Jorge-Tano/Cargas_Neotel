"""
Test de procesamiento - REFI y PL Leakage (descarga desde SFTP)
Ejecutar desde backend\:
    python test_refi_pl.py
"""

import sys
import os
import warnings
warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(BASE_DIR, ".env"))

import pandas as pd
from app.core.ftp import descargar_archivo_sftp
from app.services.refi_pl import procesar_refi_pl

for tipo in ["REFI", "PL"]:
    print("=" * 55)
    print(f"  TEST - {tipo} LEAKAGE")
    print("=" * 55)

    try:
        print(f"\n[1/3] Descargando archivo {tipo} desde SFTP...")
        archivo_bytes, nombre_archivo = descargar_archivo_sftp(tipo)
        print(f"  Archivo: {nombre_archivo}")

        print(f"\n[2/3] Procesando {tipo}...")
        resultado = procesar_refi_pl(
            tipo=tipo,
            output_dir=BASE_DIR,
            archivo_bytes=archivo_bytes,
            nombre_archivo=nombre_archivo,
        )

        print(f"  Total entrada    : {resultado['total_entrada']}")
        print(f"  Repetidos        : {resultado['total_repetidos']}")
        print(f"  Bloqueados (LN)  : {resultado['total_bloqueados']}")
        print(f"  Total carga      : {resultado['total_carga']}")

        print(f"\n[3/3] Archivos generados:")
        for key, path in resultado.items():
            if key.startswith("archivo") and os.path.exists(path):
                filas = len(pd.read_excel(path, engine="xlrd", dtype=str))
                print(f"  {os.path.basename(path)} → {filas} filas")

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback; traceback.print_exc()

    print()

print("=" * 55)
print("  Listo!")
print("=" * 55 + "\n")