from src.core import acm_extractors, acm_transformers, acm_loaders, acm_utils
from config.acm_settings import settings

def run_master_data_population_with_logging():
    engine = acm_utils.get_db_engine()
    log_id = None
    registros = {}
    try:
        log_id = acm_utils.iniciar_log(engine, 'populate_masters', 'operations')
        
        # Poblar Temporadas y SubTemporadas (Esto se asume que se hace manualmente en la BD por ahora)
        print("Poblando datos maestros desde el archivo de Obras...")
        df_obras_raw = acm_extractors.extract_from_excel(settings.operations.obras_path, sheet_name="Obras", skiprows=5)

        # --- INICIO DE CÓDIGO DE DIAGNÓSTICO DE COLUMNAS ---
        """ print("\n--- Columnas encontradas en '1. Obras.xlsx' ---")
        print(df_obras_raw.columns.tolist())
        print("------------------------------------------------") """
        # --- FIN DE CÓDIGO DE DIAGNÓSTICO ---
        
        # Cargar Canales
        df_canales = acm_transformers.transform_canales_from_obras(df_obras_raw)
        registros['canales'] = acm_loaders.load_to_acm_database(df_canales, "canales", engine, unique_column='nombre')
        
        # Cargar Materiales
        df_materiales = acm_transformers.transform_materiales_from_obras_columns(df_obras_raw)
        registros['materiales'] = acm_loaders.load_to_acm_database(df_materiales, "materiales", engine, unique_column='nombre')

        acm_utils.finalizar_log(engine, log_id, 'Exitoso', registros)
        print(f"✅ Población de datos maestros de Operaciones completada. Resumen: {registros}")

    except Exception as e:
        print(f"❌ ERROR en la población de datos maestros de Operaciones: {e}")
        if log_id:
            acm_utils.finalizar_log(engine, log_id, 'Fallido', mensaje_error=str(e))

def run_transactional_etl_with_logging():
    engine = acm_utils.get_db_engine()
    log_id = None
    registros = {}
    try:
        log_id = acm_utils.iniciar_log(engine, 'run_transactions', 'operations')

        subtemporadas_df = acm_utils.get_subtemporadas(engine)
        if subtemporadas_df.empty:
            raise ValueError("La tabla 'SubTemporadas' está vacía. Por favor, puebla los datos maestros primero.")

        # Procesar Obras y Materiales
        df_obras_raw = acm_extractors.extract_from_excel(settings.operations.obras_path, sheet_name="Obras")
        transformed_obras = acm_transformers.transform_obras_and_materiales(df_obras_raw, engine, subtemporadas_df)
        registros['obras'] = acm_loaders.load_to_acm_database(transformed_obras['obras'], "Obras", engine, unique_column='codigo_obra')
        registros['materiales_en_obra'] = acm_loaders.load_to_acm_database(transformed_obras['materiales_en_obra'], "MaterialesEnObra", engine)
        
        # (Aquí añadirías la lógica para Labores y Bodega de forma similar)

        acm_utils.finalizar_log(engine, log_id, 'Exitoso', registros)
        print(f"✅ Carga transaccional de Operaciones completada. Resumen: {registros}")
        
    except Exception as e:
        print(f"❌ ERROR en el ETL transaccional de Operaciones: {e}")
        if log_id:
            acm_utils.finalizar_log(engine, log_id, 'Fallido', mensaje_error=str(e))