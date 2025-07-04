# src/core/acm_loaders.py
import pandas as pd
from sqlalchemy import types # <--- IMPORTANTE: Añadir esta importación

def load_to_acm_database(df, table_name, engine, if_exists='append', index=False, unique_column=None):
    """
    Carga un DataFrame, asegurando la transacción y forzando el tipo de dato
    de la columna 'codigo' a String al escribir en la base de datos.
    """
    if df is None or df.empty:
        print(f"No hay datos para cargar en la tabla '{table_name}'.")
        return 0
    try:
        with engine.begin() as conn:
            # --- CAMBIO CLAVE: DEFINIR EL TIPO DE DATO PARA LA ESCRITURA ---
            # Creamos un diccionario para forzar el tipo de las columnas.
            # Le decimos a SQLAlchemy: "La columna 'codigo' es SIEMPRE un String".
            dtype_map = {'codigo': types.String}

            if unique_column:
                existing_values_df = pd.read_sql(f'SELECT "{unique_column}" FROM "{table_name}"', conn)
                existing_values = existing_values_df[unique_column].tolist()
                df_to_load = df[~df[unique_column].isin(existing_values)]
                new_rows = len(df_to_load)
                if new_rows == 0:
                    print(f"Todos los registros para '{table_name}' ya existen en la base de datos.")
                    return 0
                print(f"Cargando {new_rows} de {len(df)} registros nuevos en '{table_name}'.")
            else:
                df_to_load = df

            # Pasamos el mapa de tipos de dato a la función to_sql
            df_to_load.to_sql(
                table_name, 
                con=conn, 
                if_exists=if_exists, 
                index=index, 
                dtype=dtype_map # <--- AQUÍ APLICAMOS LA REGLA
            )
            return len(df_to_load)
            
    except Exception as e:
        print(f"Error al cargar datos en la tabla '{table_name}': {e}")
        raise