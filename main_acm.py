import argparse
import os
import sys
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Añade la carpeta 'src' al path de Python para que encuentre los módulos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Importar los módulos de cada área después de cargar las variables
from areas.acm_operations import etl_acm_operations

def main():
    """
    Orquestador principal de la suite de ETL para la Asociación Canal Maule.
    """
    parser = argparse.ArgumentParser(description="Suite de ETL para la ACM.")
    
    parser.add_argument(
        '--task',
        choices=['populate_masters', 'run_transactions'],
        required=True,
        help="El tipo de trabajo a realizar."
    )
    
    parser.add_argument(
        '--area',
        choices=['operations'],
        default='operations',
        help="El área de negocio a procesar."
    )

    args = parser.parse_args()

    # Lógica de selección
    if args.area == 'operations':
        print(f"🚀 Ejecutando tarea '{args.task}' para el área de OPERACIONES...")
        if args.task == 'populate_masters':
            etl_acm_operations.run_master_data_population_with_logging()
        elif args.task == 'run_transactions':
            etl_acm_operations.run_transactional_etl_with_logging()
    
    else:
        print(f"El área '{args.area}' no tiene un proceso ETL definido.")

if __name__ == "__main__":
    main()