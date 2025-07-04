# src/core/acm_utils.py
import os
import sqlalchemy
import json
import pandas as pd
from sqlalchemy import create_engine, text

def get_db_engine():
    """Crea y devuelve un engine de SQLAlchemy para la conexión a la BD usando las credenciales del .env."""
    try:
        db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        return create_engine(db_url)
    except Exception as e:
        print(f"Error al crear la conexión a la base de datos: {e}")
        raise

def get_subtemporadas(engine):
    """Obtiene las subtemporadas de la base de datos para usarlas en las transformaciones."""
    return pd.read_sql("SELECT id_subtemporada, fecha_inicio, fecha_fin FROM subtemporadas", engine)

def iniciar_log(engine, tipo_tarea, area):
    """Inserta una nueva fila en logetl marcando el inicio del proceso y devuelve el ID."""
    with engine.begin() as conn:
        result = conn.execute(text(
            "INSERT INTO logetl (tipo_tarea, area_negocio, estado) VALUES (:tipo, :area, 'Iniciado') RETURNING id_log"
        ), {"tipo": tipo_tarea, "area": area})
        return result.scalar_one()

def finalizar_log(engine, log_id, estado, registros=None, mensaje_error=None):
    """Actualiza la fila del log con el resultado final del proceso (Exitoso o Fallido)."""
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE logetl SET timestamp_fin = CURRENT_TIMESTAMP, estado = :estado, registros_procesados = :registros, mensaje_error = :error WHERE id_log = :id"
        ), {"estado": estado, "registros": json.dumps(registros) if registros else None, "error": mensaje_error, "id": log_id})