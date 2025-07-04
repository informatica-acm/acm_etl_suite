# src/config/acm_settings.py

from pathlib import Path
import os

# --- CÁLCULO DE RUTA CON MÉTODO DIRECTO (CORREGIDO) ---
#
# Usamos os.getcwd(), que obtiene la carpeta desde donde se ejecutó el comando 'python'.
# Como siempre ejecutamos desde la raíz 'acm_etl_suite/', esta será nuestra ruta base.
# Este método es más directo y debería resolver el problema de cálculo de una vez por todas.
#
BASE_DIR = Path(os.getcwd())

# Construimos la ruta a la carpeta de datos, que está DENTRO del proyecto.
DATA_DIR = BASE_DIR / "data_acm"


class OperationsSettings:
    """Define las rutas a los archivos Excel para el área de operaciones."""
    obras_path: Path = DATA_DIR / "raw_acm" / "1. Obras.xlsx"
    labores_path: Path = DATA_DIR / "raw_acm" / "2. Labores.xlsx"
    codigos_path: Path = DATA_DIR / "raw_acm" / "Códigos.xlsx"

class Settings:
    """Configuración central del proyecto."""
    operations = OperationsSettings()

settings = Settings()