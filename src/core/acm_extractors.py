# src/core/acm_extractors.py

import pandas as pd

def extract_from_excel(file_path, sheet_name=0, skiprows=0):
    """
    Extrae datos de una hoja específica de un archivo Excel.
    
    Args:
        file_path (str): La ruta al archivo Excel.
        sheet_name (str or int): El nombre o índice de la hoja a leer.
        skiprows (int): El número de filas a saltar al inicio del archivo.
    """
    try:
        # Añadimos el parámetro skiprows a la función de lectura
        return pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skiprows, engine='openpyxl')
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo en la ruta: {file_path}")
        raise
    except Exception as e:
        print(f"Error al leer el archivo Excel '{file_path}': {e}")
        raise