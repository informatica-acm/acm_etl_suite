import sys
import os

# Add the project root to the Python path
# This assumes main_acm.py is in src/, and the project root is one level up from src/
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import argparse
from src.core.acm_utils import setup_acm_logging
from src.areas.acm_operations.etl_acm_operations import run_acm_operations_etl
# from src.areas.acm_finance.etl_acm_finance import run_acm_finance_etl

logger = setup_acm_logging(__name__)

def main_acm():
    """
    Orquestador principal para ejecutar los procesos ETL de la ACM.
    Usa argumentos de línea de comandos para seleccionar el área a procesar.
    """
    parser = argparse.ArgumentParser(description="Ejecuta procesos ETL para diferentes áreas de la Asociación Canal Maule (ACM).")
    parser.add_argument('--area', type=str, required=True,
                        help="Área de la ACM para ejecutar el ETL (ej. 'operations', 'finance', 'legal')")
    args = parser.parse_args()

    if args.area == 'operations':
        logger.info("Ejecutando ETL para el área de OPERACIONES de ACM.")
        run_acm_operations_etl()
    # elif args.area == 'finance':
    #     logger.info("Ejecutando ETL para el área de FINANZAS de ACM.")
    #     # run_acm_finance_etl()
    # elif args.area == 'legal':
    #     logger.info("Ejecutando ETL para el área LEGAL de ACM.")
    #     # run_acm_legal_etl()
    else:
        logger.error(f"Área '{args.area}' no reconocida o no implementada para ACM. Por favor, elige entre 'operations', 'finance', 'legal'.")
        parser.print_help()

if __name__ == "__main__":
    main_acm()