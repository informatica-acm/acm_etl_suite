import os
import sqlalchemy
import json
import pandas as pd
from sqlalchemy import create_engine, text

def get_db_engine():
    """Crea y devuelve un engine de SQLAlchemy para la conexión a la BD."""
    try:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        
        if not all([db_user, db_password, db_host, db_port, db_name]):
            raise ValueError("Faltan variables de entorno para la conexión a la base de datos.")
            
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return create_engine(db_url)
    except Exception as e:
        print(f"Error al crear la conexión a la base de datos: {e}")
        raise

def get_subtemporadas(engine):
    """Obtiene las subtemporadas de la base de datos para usarlas en las transformaciones."""
    return pd.read_sql("SELECT id_subtemporada, fecha_inicio, fecha_fin FROM SubTemporadas", engine)

def iniciar_log(engine, tipo_tarea, area):
    """Inserta una nueva fila en LogETL marcando el inicio del proceso y devuelve el ID."""
    with engine.connect() as conn:
        result = conn.execute(text(
            """
            INSERT INTO LogETL (tipo_tarea, area_negocio, estado) 
            VALUES (:tipo, :area, 'Iniciado') RETURNING id_log
            """
        ), {"tipo": tipo_tarea, "area": area})
        conn.commit()
        return result.scalar_one()

def finalizar_log(engine, log_id, estado, registros=None, mensaje_error=None):
    """Actualiza la fila del log con el resultado final del proceso."""
    with engine.connect() as conn:
        conn.execute(text(
            """
            UPDATE LogETL SET
                timestamp_fin = CURRENT_TIMESTAMP,
                estado = :estado,
                registros_procesados = :registros,
                mensaje_error = :error
            WHERE id_log = :id
            """
        ), {
            "estado": estado,
            "registros": json.dumps(registros) if registros else None,
            "error": mensaje_error,
            "id": log_id
        })
        conn.commit()