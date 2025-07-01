import pandas as pd
from src.core.acm_utils import setup_acm_logging
import os
from datetime import datetime

logger = setup_acm_logging(__name__)

def load_to_acm_database(df, table_name, engine, if_exists='append'):
    """
    Carga datos de ACM a una tabla específica en una base de datos (ej. Supabase).
    El 'engine' debe ser un motor SQLAlchemy previamente conectado.
    'if_exists': 'append' (añadir nuevas filas), 'replace' (reemplazar tabla), 'fail' (fallar si existe).
    """
    if df is None or df.empty:
        logger.warning(f"No hay datos de ACM para cargar en la tabla '{table_name}'.")
        return False
    try:
        # 'method=multi' es más eficiente para inserciones grandes en PostgreSQL
        # chunksize ayuda a insertar en lotes más pequeños para manejar la memoria y transacciones
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, method='multi', chunksize=1000)
        logger.info(f"Datos ACM cargados exitosamente a la tabla '{table_name}' en DB. Filas: {len(df)}")
        return True
    except Exception as e:
        logger.error(f"Error al cargar datos de ACM a la base de datos '{table_name}': {e}")
        return False

def load_to_acm_csv(df, base_file_name, output_path, version_strategy='timestamp'):
    """
    Carga datos de ACM a un archivo CSV en el sistema de archivos con estrategia de versionado.
    'version_strategy': 'timestamp' para añadir fecha/hora al nombre del archivo,
                       cualquier otra cosa para sobrescribir (nombre base).
    """
    if df is None or df.empty:
        logger.warning("No hay datos de ACM para cargar en el archivo CSV.")
        return False
    try:
        if version_strategy == 'timestamp':
            timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S') # Formato: YYYY-MM-DD_HHMMSS
            file_name = f"{base_file_name}_{timestamp}.csv"
        else:
            file_name = f"{base_file_name}.csv"

        full_file_path = os.path.join(output_path, file_name)
        df.to_csv(full_file_path, index=False)
        logger.info(f"Datos ACM cargados exitosamente al archivo versionado: '{full_file_path}'. Filas: {len(df)}")
        return True
    except Exception as e:
        logger.error(f"Error al cargar datos de ACM al CSV: {e}")
        return False