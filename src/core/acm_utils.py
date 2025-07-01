import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from config.acm_settings import LOG_FILE_ACM, LOGGING_CONFIG_ACM

# =======================
# 1. Setup de logging
# =======================

def setup_acm_logging(name):
    """
    Configura el logger para un módulo específico del proyecto ACM.
    Los logs se escribirán en un archivo y en la consola.
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOGGING_CONFIG_ACM['level'])

    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_FILE_ACM)
        file_handler.setFormatter(logging.Formatter(LOGGING_CONFIG_ACM['format']))
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(LOGGING_CONFIG_ACM['format']))
        logger.addHandler(stream_handler)

    return logger

# =======================
# 2. Setup del motor DB
# =======================

load_dotenv()

SUPABASE_DB_TYPE = os.getenv("SUPABASE_DB_TYPE")
SUPABASE_DB_USER = os.getenv("SUPABASE_DB_USER")
SUPABASE_DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
SUPABASE_DB_HOST = os.getenv("SUPABASE_DB_HOST")
SUPABASE_DB_PORT = os.getenv("SUPABASE_DB_PORT")
SUPABASE_DB_NAME = os.getenv("SUPABASE_DB_NAME")

def get_acm_db_engine(db_type, db_user, db_pass, db_host, db_port, db_name):
    try:
        db_port = int(db_port)
        connection_str = f"{db_type}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_str)
        logging.getLogger(__name__).info("Motor de conexión a base de datos creado exitosamente.")
        return engine
    except Exception as e:
        logging.getLogger(__name__).error(f"Error al crear motor de DB para ACM: {e}")
        return None

# Motor global para toda la suite ETL
supabase_engine = get_acm_db_engine(
    SUPABASE_DB_TYPE,
    SUPABASE_DB_USER,
    SUPABASE_DB_PASSWORD,
    SUPABASE_DB_HOST,
    SUPABASE_DB_PORT,
    SUPABASE_DB_NAME
)
