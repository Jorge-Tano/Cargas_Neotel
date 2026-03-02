import sys; sys.path.insert(0, '.')
from dotenv import load_dotenv; load_dotenv()
import pandas as pd
from app.services.sav_av import _normalizar_columnas
from app.services.utils import separar_lista_negra, aplicar_contacto_efectivo
from app.core.postgres import get_lista_negra
from app.core.sqlserver import get_repetidos, get_contactos_efectivos_5757

df_original = pd.read_excel('CARGA_SAV_LEAKAGE_28022026.xlsx', dtype=str)
df_original.columns = df_original.columns.str.strip()
df = _normalizar_columnas(df_original.copy(), 'SAV')

ruts_repetidos = get_repetidos('SAV_AV')
ruts_archivo = df['RUT'].astype(str).str.strip().tolist()
idx_nuevos = [i for i, r in enumerate(ruts_archivo) if r not in ruts_repetidos]

df_nuevos = df.iloc[idx_nuevos].reset_index(drop=True)
contactos = get_contactos_efectivos_5757()
df_nuevos = aplicar_contacto_efectivo(df_nuevos, 'RUT', 'TELEFONO_1', contactos)
df_nuevos = df_nuevos.reset_index(drop=True)

lista_negra = get_lista_negra()
df_limpios, df_bloq = separar_lista_negra(df_nuevos, 'TELEFONO_1', lista_negra)

print('Bloqueados:', len(df_bloq))
if len(df_bloq) > 0:
    for i in df_bloq.index.tolist():
        fono_norm = df_bloq.loc[i, 'TELEFONO_1']
        orig_i = idx_nuevos[i] if i < len(idx_nuevos) else -1
        fono_orig = df_original.iloc[orig_i]['Telefono'] if orig_i >= 0 else '?'
        en_ln = fono_norm in lista_negra
        print(f'idx_norm={i} idx_orig={orig_i} fono_norm={fono_norm} fono_orig={fono_orig} en_LN={en_ln}')
