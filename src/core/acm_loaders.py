# src/core/acm_loaders.py

import pandas as pd

def load_to_acm_database(df, table_name, engine, if_exists='append', index=False, unique_column=None):
    """
    Carga un DataFrame en una tabla de la base de datos, asegurando que la transacción se confirme.
    Devuelve el número de filas cargadas.
    """
    if df is None or df.empty:
        print(f"No hay datos para cargar en la tabla '{table_name}'.")
        return 0

    try:
        # --- ESTE ES EL CAMBIO CLAVE ---
        # El bloque 'with engine.begin() as conn:' asegura que al final,
        # si no hay errores, se ejecute un COMMIT para guardar los cambios.
        with engine.begin() as conn:
            if unique_column:
                # Leemos los datos existentes dentro de la misma transacción
                existing_values_df = pd.read_sql(f'SELECT "{unique_column}" FROM "{table_name}"', conn)
                existing_values = existing_values_df[unique_column].tolist()
                
                original_rows = len(df)
                df_to_load = df[~df[unique_column].isin(existing_values)]
                new_rows = len(df_to_load)
                
                if new_rows == 0:
                    print(f"Todos los registros para '{table_name}' ya existen en la base de datos.")
                    return 0
                print(f"Cargando {new_rows} de {original_rows} registros nuevos en '{table_name}'.")
            else:
                df_to_load = df

            # La operación de escritura se hace dentro del bloque de transacción
            df_to_load.to_sql(table_name, con=conn, if_exists=if_exists, index=index)
            return len(df_to_load)
            
    except Exception as e:
        print(f"Error al cargar datos en la tabla '{table_name}': {e}")
        raise