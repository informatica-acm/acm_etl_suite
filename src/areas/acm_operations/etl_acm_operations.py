import pandas as pd
from datetime import datetime
import logging

from src.core.acm_extractors import extract_from_acm_excel
from src.core.acm_transformers import (
    clean_acm_data,
    standardize_column_names,
    transform_obras,
    transform_labores,
    transform_bodega,
)
from src.core.acm_loaders import load_to_acm_database
from src.areas.acm_operations.config_acm_operations import ACM_OPERATIONS_INPUTS
from src.core.acm_utils import supabase_engine

# Configurar logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def run_acm_operations_etl():
    logger.info("Iniciando ETL para el área de Operaciones de ACM...")

    if not supabase_engine:
        logger.error("No se pudo establecer conexión con Supabase. Abortando ETL de Operaciones.")
        return

    # Transformadores por archivo
    transformadores = {
        "obras": transform_obras,
        "labores": transform_labores,
        "bodega": transform_bodega,
    }

    for config in ACM_OPERATIONS_INPUTS:
        logger.info(f"Procesando {config['nombre_logico'].upper()} desde {config['excel_path']}...")

        # 1. Extraer
        df_raw = extract_from_acm_excel(config["excel_path"], sheet_name=config["sheet"])
        if df_raw is None:
            logger.warning(f"No se pudieron extraer datos de {config['nombre_logico']}. Saltando.")
            continue

        # 2. Limpiar base
        df_clean = clean_acm_data(df_raw.copy())
        df_clean = standardize_column_names(df_clean)

        # 3. Transformar a uno o más DataFrames
        transformador = transformadores.get(config["transformador"])
        if not transformador:
            logger.warning(f"No hay transformador definido para '{config['nombre_logico']}'. Saltando.")
            continue

        resultados = transformador(df_clean)
        if not isinstance(resultados, dict):
            logger.error(f"Transformador '{config['transformador']}' no devolvió un diccionario.")
            continue

        # 4. Cargar cada tabla destino
        for tabla_cfg in config.get("tablas_destino", []):
            df_result = resultados.get(tabla_cfg["df_key"])
            if df_result is None or df_result.empty:
                logger.warning(f"DataFrame '{tabla_cfg['df_key']}' vacío o no encontrado. Saltando carga.")
                continue

            if tabla_cfg.get("fecha_col") in df_result.columns:
                df_result[tabla_cfg["fecha_col"]] = pd.to_datetime(df_result[tabla_cfg["fecha_col"]], errors="coerce")

            df_result["fecha_reporte"] = datetime.now().date()

            logger.info(f"Cargando '{tabla_cfg['df_key']}' en tabla '{tabla_cfg['tabla']}'...")
            load_to_acm_database(df_result, tabla_cfg["tabla"], supabase_engine, if_exists="append")

    logger.info("ETL de Operaciones completado exitosamente.")
