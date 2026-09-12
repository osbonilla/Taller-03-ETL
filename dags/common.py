"""
Módulo de configuración compartida para los DAGs del Taller 03 - ETL.

Centraliza aspectos que varían entre entornos (rutas de datos, identificador
de la conexión de PostgreSQL) mediante variables de entorno, siguiendo el
principio de separar la configuración de la lógica de negocio. De esta forma
los cinco DAGs reutilizan las mismas rutas y no duplican estas constantes.

Al residir directamente en la carpeta ``dags/``, Airflow lo agrega
automáticamente a ``sys.path`` durante el parseo de DAGs, por lo que puede
importarse desde cualquier archivo de esa carpeta con ``import common`` o
``from common import ...`` sin configuración adicional.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas de datos
# ---------------------------------------------------------------------------
# Dentro de los contenedores de Airflow, ./data del repositorio se monta en
# /opt/airflow/data (ver volumes en docker-compose.yml). La variable de
# entorno TALLER_DATA_DIR permite sobreescribir esta ruta -por ejemplo, para
# pruebas locales fuera de Docker- sin modificar el código de los DAGs.
DATA_DIR = Path(os.environ.get("TALLER_DATA_DIR", "/opt/airflow/data"))

RAW_DIR = DATA_DIR / "raw"
INCOMING_DIR = RAW_DIR / "incoming"
PROCESSED_DIR = DATA_DIR / "processed"
QUARANTINE_DIR = DATA_DIR / "quarantine"
BACKUPS_DIR = DATA_DIR / "backups"
SAMPLE_DIR = DATA_DIR / "sample"

ALL_DATA_DIRS = (
    RAW_DIR,
    INCOMING_DIR,
    PROCESSED_DIR,
    QUARANTINE_DIR,
    BACKUPS_DIR,
    SAMPLE_DIR,
)

# ---------------------------------------------------------------------------
# Conexión a base de datos
# ---------------------------------------------------------------------------
# Identificador de la Connection de Airflow (Admin > Connections en la UI,
# o variable de entorno AIRFLOW_CONN_POSTGRES_TALLER) usada por los DAGs para
# leer/escribir datos de negocio. Se reutiliza el mismo servidor PostgreSQL
# que Airflow ya requiere para sus propios metadatos, pero bajo un esquema
# separado ("taller"), evitando levantar un segundo motor de base de datos
# innecesario para un proyecto de taller.
POSTGRES_CONN_ID = os.environ.get("TALLER_POSTGRES_CONN_ID", "postgres_taller")
POSTGRES_SCHEMA = os.environ.get("TALLER_POSTGRES_SCHEMA", "taller")

# ---------------------------------------------------------------------------
# Utilidades comunes
# ---------------------------------------------------------------------------


def ensure_data_dirs() -> None:
    """Crea (si no existen) todas las carpetas de datos usadas por los DAGs.

    Es idempotente: puede invocarse al inicio de cualquier tarea sin riesgo
    de error si las carpetas ya existen.
    """
    for directory in ALL_DATA_DIRS:
        directory.mkdir(parents=True, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    """Devuelve un logger estándar de Python.

    Airflow ya captura la salida del logger raíz y la muestra en los logs de
    cada task instance dentro de la UI, por lo que no requiere configuración
    adicional de handlers.
    """
    return logging.getLogger(name)
