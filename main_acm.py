# main_acm.py
import argparse
import os
import sys
from dotenv import load_dotenv

# Carga las variables de entorno (credenciales) desde el archivo .env
load_dotenv()

# Añade la carpeta 'src' al path de Python para que pueda encontrar los módulos
# Esto es crucial para que las importaciones funcionen correctamente.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Importa el módulo específico del ETL para el área de operaciones
from areas.acm_operations import etl_acm_operations

def main():
    """
    Orquestador principal de la suite de ETL para la Asociación Canal Maule.
    Lee los argumentos de la línea de comandos para decidir qué proceso ejecutar.
    """
    parser = argparse.ArgumentParser(description="Suite de ETL para la ACM.")
    
    # Argumento para definir la TAREA a realizar
    parser.add_argument(
        '--task',
        choices=['populate_masters', 'run_transactions'],
        required=True,
        help="Define la TAREA: 'populate_masters' para carga inicial, 'run_transactions' para carga periódica."
    )
    
    # Argumento para definir el ÁREA de negocio a procesar
    parser.add_argument(
        '--area',
        choices=['operations'],
        required=True,
        help="Define el ÁREA de negocio a procesar."
    )
    args = parser.parse_args()

    # Lógica de selección por área para decidir qué módulo de ETL llamar
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

# source venv/Scripts/activate
# python main_acm.py --task populate_masters --area operations
# python main_acm.py --task run_transactions --area operations