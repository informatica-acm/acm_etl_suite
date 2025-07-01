import pandas as pd
from src.core.acm_utils import setup_acm_logging

logger = setup_acm_logging(__name__)

def clean_acm_data(df):
    """
    Realiza una limpieza básica de datos para DataFrames de ACM.
    - Elimina filas duplicadas.
    - Puedes añadir más lógica de limpieza aquí (ej. manejo de nulos, estandarización de texto).
    """
    if df is None:
        logger.warning("DataFrame de entrada es None para limpieza. Retornando None.")
        return None
    
    initial_rows = len(df)
    df_cleaned = df.drop_duplicates().copy() # Usar .copy() para evitar SettingWithCopyWarning
    
    if initial_rows != len(df_cleaned):
        logger.info(f"Limpieza de datos ACM: {initial_rows - len(df_cleaned)} duplicados eliminados.")
    
    # Ejemplo de manejo de nulos: rellenar con un valor por defecto o hacia adelante/atrás
    # df_cleaned.fillna(method='ffill', inplace=True) 
    # df_cleaned['columna_numerica'] = pd.to_numeric(df_cleaned['columna_numerica'], errors='coerce')
    # df_cleaned['columna_texto'] = df_cleaned['columna_texto'].str.strip().str.lower()

    return df_cleaned

def aggregate_acm_data(df, group_by_column, aggregate_column, agg_func='sum'):
    """
    Realiza una agregación básica de datos para DataFrames de ACM.
    Agrupa por una o más columnas y aplica una función de agregación.
    """
    if df is None:
        logger.warning("DataFrame de entrada es None para agregación. Retornando None.")
        return None
    
    try:
        aggregated_df = df.groupby(group_by_column)[aggregate_column].agg(agg_func).reset_index()
        logger.info(f"Datos ACM agregados por '{group_by_column}' usando '{agg_func}'.")
        return aggregated_df
    except KeyError as e:
        logger.error(f"Error al agregar datos: Columna no encontrada - {e}. Asegúrate de que las columnas existan.")
        return None
    except Exception as e:
        logger.error(f"Error inesperado durante la agregación: {e}")
        return None

# Puedes añadir más funciones de transformación aquí (ej. estandarización de nombres, cálculos específicos)
def standardize_column_names(df):
    """Estandariza los nombres de las columnas a un formato snake_case."""
    if df is None:
        return None
    original_cols = df.columns.tolist()
    new_cols = []
    for col in original_cols:
        new_col = col.strip().lower().replace(' ', '_').replace('-', '_')
        new_cols.append(new_col)
    df.columns = new_cols
    logger.info(f"Nombres de columnas estandarizados: {original_cols} -> {new_cols}")
    return df

def transform_obras(df):
    """
    Transforma el DataFrame de obras y devuelve un diccionario con el DataFrame principal.
    """
    # Aquí puedes aplicar lógica real si es necesario
    return {"df_obras": df}

def transform_labores(df):
    """
    Transforma el DataFrame de labores y lo divide en labores y materiales.
    """
    df_labores = df[["id_labor", "canal_id", "codigo_labor", "fecha_labor", "responsable"]].copy()
    df_materiales = df[["id_labor", "material", "cantidad", "costo_unitario"]].copy()

    return {
        "df_labores": df_labores,
        "df_materiales": df_materiales
    }

def transform_bodega(df):
    """
    Transforma los datos de bodega.
    """
    return {"df_bodega": df}