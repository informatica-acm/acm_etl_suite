import os

# Ruta al directorio donde están los archivos de Excel
DATA_RAW_ACM_PATH = os.path.join(os.getcwd(), "data_acm", "raw_acm")

# Configuración de entradas para ETL de operaciones (mantención de canales)
ACM_OPERATIONS_INPUTS = [
    {
        "nombre_logico": "obras",
        "excel_path": os.path.join(DATA_RAW_ACM_PATH, "1. Obras.xlsx"),
        "sheet": "Obras",
        "transformador": "obras",
        "tablas_destino": [
            {"df_key": "df_obras", "tabla": "obras", "fecha_col": "fecha_inicio"}
        ],
    },
    {
        "nombre_logico": "labores",
        "excel_path": os.path.join(DATA_RAW_ACM_PATH, "2. Labores.xlsx"),
        "sheet": "Labores",
        "transformador": "labores",
        "tablas_destino": [
            {"df_key": "df_labores", "tabla": "labores", "fecha_col": "fecha_labor"},
            {"df_key": "df_materiales", "tabla": "materiales_utilizados", "fecha_col": "fecha_labor"}
        ],
    },
    {
        "nombre_logico": "bodega",
        "excel_path": os.path.join(DATA_RAW_ACM_PATH, "3. Bodega.xlsx"),
        "sheet": "Movimientos",
        "transformador": "bodega",
        "tablas_destino": [
            {"df_key": "df_bodega", "tabla": "movimientos_bodega", "fecha_col": "fecha_movimiento"}
        ],
    },
]
