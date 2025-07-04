# src/areas/acm_operations/etl_acm_operations.py
from src.core import acm_extractors, acm_transformers, acm_loaders, acm_utils
from config.acm_settings import settings

def run_master_data_population_with_logging():
    """Tarea para poblar las tablas maestras desde el archivo central 'Códigos.xlsx'."""
    engine = acm_utils.get_db_engine()
    log_id = None
    registros = {}
    try:
        log_id = acm_utils.iniciar_log(engine, 'populate_masters', 'operations')
        
        # Mapeo central que define qué hoja del Excel corresponde a qué tabla
        # de la base de datos y cómo se deben renombrar las columnas.
        # ¡IMPORTANTE! Ajusta las claves (ej. "CANAL") a los nombres exactos de las columnas en tu Excel.
        master_data_map = {
            "oficinas": {"sheet": "OFICINAS", "cols": {"CODIGO": "codigo", "OFICINA": "nombre"}},
            "bodegas": {"sheet": "BODEGAS", "cols": {"CODIGO": "codigo", "BODEGA": "nombre"}},
            "canales": {"sheet": "CANALES", "cols": {"CODIGO": "codigo", "CANAL": "nombre", "TIPO": "tipo"}},
            "insumos": {"sheet": "INSUMOS", "cols": {"CODIGO": "codigo", "INSUMOS": "nombre", "UNIDAD": "unidad"}},
            # Añade aquí más tablas maestras que quieras poblar desde 'Códigos.xlsx'
        }

        # Itera sobre el mapa y procesa cada tabla maestra.
        for table, config in master_data_map.items():
            print(f"Poblando tabla '{table}'...")
            df_raw = acm_extractors.extract_from_excel(settings.operations.codigos_path, sheet_name=config["sheet"])
            df_transformed = acm_transformers.transform_from_sheet(df_raw, config["cols"])
            registros[table] = acm_loaders.load_to_acm_database(df_transformed, table, engine, unique_column='codigo')

        # Finaliza el log como exitoso.
        acm_utils.finalizar_log(engine, log_id, 'Exitoso', registros)
        print(f"✅ Población de datos maestros completada. Resumen: {registros}")

    except Exception as e:
        print(f"❌ ERROR en la población de datos maestros: {e}")
        if log_id:
            acm_utils.finalizar_log(engine, log_id, 'Fallido', mensaje_error=str(e))

def run_transactional_etl_with_logging():
    """Tarea para cargar datos periódicos como Obras y sus materiales usados."""
    engine = acm_utils.get_db_engine()
    log_id = None
    registros = {}
    try:
        log_id = acm_utils.iniciar_log(engine, 'run_transactions', 'operations')
        
        # Obtiene las subtemporadas desde la BD para poder asignar las obras
        subtemporadas_df = acm_utils.get_subtemporadas(engine)
        if subtemporadas_df.empty:
            raise ValueError("La tabla 'subtemporadas' está vacía. Debes poblarla manualmente primero.")

        # Procesa el archivo de Obras
        print("Procesando Obras y Materiales en Obra...")
        df_obras_raw = acm_extractors.extract_from_excel(settings.operations.obras_path, sheet_name="Obras", skiprows=5)
        
        # Llama a la función que transforma las obras y despivota los materiales
        transformed = acm_transformers.transform_obras_and_materiales(df_obras_raw, engine, subtemporadas_df)
        
        # Carga las obras y luego los materiales asociados
        registros['obras'] = acm_loaders.load_to_acm_database(transformed['obras'], "obras", engine, unique_column='codigo_obra')
        registros['materialesenobra'] = acm_loaders.load_to_acm_database(transformed['materiales_en_obra'], "materialesenobra", engine)
        
        # (Aquí se añadiría la lógica para procesar '2. Labores.xlsx' de forma similar)

        acm_utils.finalizar_log(engine, log_id, 'Exitoso', registros)
        print(f"✅ Carga transaccional de Operaciones completada. Resumen: {registros}")
        
    except Exception as e:
        print(f"❌ ERROR en el ETL transaccional: {e}")
        if log_id:
            acm_utils.finalizar_log(engine, log_id, 'Fallido', mensaje_error=str(e))