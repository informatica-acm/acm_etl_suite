import os
from dotenv import load_dotenv

load_dotenv() # Carga las variables de entorno desde el archivo .env

# --- Rutas de Archivos y Directorios ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_ACM_PATH = os.path.join(BASE_DIR, 'data_acm', 'raw_acm')
DATA_PROCESSED_ACM_PATH = os.path.join(BASE_DIR, 'data_acm', 'processed_acm')
LOG_FILE_ACM = os.path.join(BASE_DIR, 'logs_acm', 'acm_etl.log')

# Asegúrate de que los directorios existan
os.makedirs(DATA_RAW_ACM_PATH, exist_ok=True)
os.makedirs(DATA_PROCESSED_ACM_PATH, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE_ACM), exist_ok=True)

# --- Configuración de Logging ---
LOGGING_CONFIG_ACM = {
    'level': os.getenv('LOG_LEVEL', 'INFO'), # Nivel de log desde .env, por defecto INFO
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}

# --- Credenciales de Supabase (PostgreSQL) ---
SUPABASE_DB_TYPE = os.getenv('SUPABASE_DB_TYPE', 'postgresql')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT', '5432')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')
SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')

# Validar que las credenciales críticas no estén vacías
if not all([SUPABASE_DB_HOST, SUPABASE_DB_NAME, SUPABASE_DB_USER, SUPABASE_DB_PASSWORD]):
    print("ADVERTENCIA: Las credenciales de Supabase no están completamente configuradas en el archivo .env")
    print("Asegúrate de configurar SUPABASE_DB_HOST, SUPABASE_DB_NAME, SUPABASE_DB_USER, SUPABASE_DB_PASSWORD.")