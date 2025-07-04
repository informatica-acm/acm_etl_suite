# src/core/acm_extractors.py
import pandas as pd

def extract_from_excel(file_path, sheet_name=0, skiprows=0):
    """
    Extrae datos de una hoja Excel, forzando que todas las columnas
    sean leídas como texto (string) para preservar formatos como '0206'.
    """
    try:
        # dtype=str le dice a pandas: "No adivines. Lee todo como texto."
        return pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            skiprows=skiprows,
            engine='openpyxl',
            dtype=str  # Forzar todo a string desde el origen
        )
    except Exception as e:
        print(f"Error al leer el archivo Excel '{file_path}': {e}")
        raise