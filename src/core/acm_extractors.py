import pandas as pd
from sqlalchemy import create_engine
from src.core.acm_utils import setup_acm_logging
import os

logger = setup_acm_logging(__name__)

def get_acm_db_engine(db_type, db_user, db_pass, db_host, db_port, db_name):
    """
    Crea un motor de conexión a la base de datos (ej. Supabase PostgreSQL) para ACM.
    Retorna el objeto Engine de SQLAlchemy.
    """
    try:
        # Validar que el puerto sea un entero válido
        try:
            db_port = int(db_port)
        except (TypeError, ValueError):
            logger.error(f"Puerto inválido para base de datos: '{db_port}'. Asegúrate de definir SUPABASE_DB_PORT como número entero en el .env.")
            return None

        db_connection_str = f"{db_type}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(db_connection_str)
        logger.debug(f"Motor de DB creado para ACM en {db_name}@{db_host}")
        return engine
    except Exception as e:
        logger.error(f"Error al crear motor de DB para ACM: {e}")
        return None


def extract_from_acm_database(engine, query=None, table_name=None):
    """
    Extrae datos para ACM desde una base de datos usando una consulta SQL o el nombre de una tabla.
    Retorna un DataFrame de Pandas.
    """
    if not engine:
        logger.error("Motor de base de datos ACM no proporcionado. No se puede extraer.")
        return None
    try:
        if query:
            df = pd.read_sql(query, engine)
            logger.info(f"Datos ACM extraídos de la base de datos con consulta. Filas: {len(df)}")
        elif table_name:
            df = pd.read_sql_table(table_name, engine)
            logger.info(f"Datos ACM extraídos de la tabla '{table_name}'. Filas: {len(df)}")
        else:
            logger.error("Se requiere una consulta SQL o un nombre de tabla para la extracción de ACM. No se puede extraer.")
            return None
        return df
    except Exception as e:
        logger.error(f"Error durante la extracción de DB para ACM: {e}")
        return None

def extract_from_acm_csv(file_path):
    """
    Extrae datos para ACM desde un archivo CSV.
    Retorna un DataFrame de Pandas.
    """
    if not os.path.exists(file_path):
        logger.error(f"Archivo CSV ACM no encontrado: {file_path}. No se puede extraer.")
        return None
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Datos ACM extraídos de '{file_path}'. Filas: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error al leer CSV para ACM '{file_path}': {e}")
        return None

def extract_from_acm_excel(file_path, sheet_name=0, header=0, **kwargs):
    """
    Extrae datos para ACM desde un archivo Excel.
    Permite especificar la hoja y la fila de encabezado.
    Retorna un DataFrame de Pandas.

    Args:
        file_path (str): Ruta al archivo Excel.
        sheet_name (str o int): Nombre de la hoja o índice (0 para la primera hoja).
        header (int o list of int): Fila(s) a usar como encabezado (0-indexado).
        **kwargs: Argumentos adicionales para pandas.read_excel (ej. skip_rows, usecols).
    """
    if not os.path.exists(file_path):
        logger.error(f"Archivo Excel ACM no encontrado: {file_path}. No se puede extraer.")
        return None
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header, **kwargs)
        logger.info(f"Datos ACM extraídos de Excel '{file_path}' (Hoja: '{sheet_name}', Encabezado en fila: {header}). Filas: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error al leer Excel para ACM '{file_path}': {e}")
        return None