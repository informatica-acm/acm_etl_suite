from pathlib import Path

# Ruta base del proyecto (CORREGIDO: ahora apunta a 'acm_etl_suite/')
# Se quitaron los print de diagnóstico
BASE_DIR = Path(__file__).resolve().parent.parent

# Ruta a la carpeta de datos (debe buscar DENTRO de la carpeta del proyecto)
DATA_DIR = BASE_DIR / "data_acm"

class OperationsSettings:
    """Rutas para los archivos del área de operaciones."""
    obras_path: Path = DATA_DIR / "raw_acm" / "1. Obras.xlsx"
    labores_path: Path = DATA_DIR / "raw_acm" / "2. Labores.xlsx"
    bodega_path: Path = DATA_DIR / "raw_acm" / "3. Bodega.xlsx"

class Settings:
    """Configuración central del proyecto."""
    operations = OperationsSettings()

settings = Settings()