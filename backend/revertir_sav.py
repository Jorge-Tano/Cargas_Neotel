"""Revierte IDDATABASE_SAV al valor correcto tras una mala detección — correr en el servidor."""
from app.core.postgres import set_config_global, get_config_valor

set_config_global({
    "IDDATABASE_SAV": "223",
    "IDDATABASE_SAV_AV_MES_CONFIRMADO": "2026-09",
})
print("IDDATABASE_SAV ahora:", get_config_valor("IDDATABASE_SAV"))
