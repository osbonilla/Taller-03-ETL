"""
DAG 1 · weather_etl_dag
=======================

Propósito
---------
Pipeline ETL que extrae el clima actual de un conjunto de ciudades desde la
API pública de Open-Meteo (sin necesidad de API key), lo transforma con
pandas y lo carga en PostgreSQL. Representa el flujo más simple del taller
(extract -> transform -> load -> summary) e introduce los conceptos base de
Airflow: DAG, task, operator, dependencia lineal, reintentos y logging.

Flujo de tareas
----------------
    extraer_datos_clima >> transformar_datos_clima >> cargar_datos_postgres >> generar_resumen

- ``extraer_datos_clima``: consulta la API REST para cada ciudad configurada
  y guarda la respuesta cruda (JSON) en ``data/raw``.
- ``transformar_datos_clima``: lee el JSON crudo, lo convierte en un
  DataFrame de pandas y agrega una columna derivada (clasificación de
  temperatura). Guarda el resultado en ``data/processed`` como CSV.
- ``cargar_datos_postgres``: inserta (o actualiza, vía upsert) el CSV
  procesado en la tabla ``taller.clima_observaciones``.
- ``generar_resumen``: consulta la tabla recién cargada y registra en el log
  un resumen agregado por clasificación de temperatura.

La ruta de un archivo (o el número de filas) generada por una tarea se
comunica a la siguiente mediante XCom (valor de retorno de la función
Python), que es la forma estándar en Airflow de pasar metadatos pequeños
entre tasks sin acoplarlas directamente.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

import common
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = common.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuración del DAG
# ---------------------------------------------------------------------------
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Ciudades a monitorear: nombre -> coordenadas (latitud, longitud).
CIUDADES = {
    "Quito": {"lat": -0.1807, "lon": -78.4678},
    "Bogota": {"lat": 4.7110, "lon": -74.0721},
    "Lima": {"lat": -12.0464, "lon": -77.0428},
    "Ciudad de Mexico": {"lat": 19.4326, "lon": -99.1332},
    "Buenos Aires": {"lat": -34.6037, "lon": -58.3816},
}

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _clasificar_temperatura(temperatura_c: float) -> str:
    """Clasifica una temperatura en grados Celsius en una categoría simple."""
    if temperatura_c < 10:
        return "frio"
    if temperatura_c < 22:
        return "templado"
    return "calido"


def extraer_datos_clima(**context) -> str:
    """Consulta Open-Meteo para cada ciudad configurada y guarda el JSON crudo.

    Devuelve la ruta del archivo generado, que la siguiente tarea recibe vía
    XCom.
    """
    common.ensure_data_dirs()
    resultados = []

    for ciudad, coords in CIUDADES.items():
        response = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "current_weather": "true",
            },
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        payload["ciudad"] = ciudad
        resultados.append(payload)
        logger.info(
            "Clima obtenido para %s: %s°C",
            ciudad,
            payload["current_weather"]["temperature"],
        )

    raw_path = common.RAW_DIR / f"clima_{context['ds_nodash']}.json"
    raw_path.write_text(json.dumps(resultados, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Archivo crudo guardado en %s (%s ciudades)", raw_path, len(resultados))
    return str(raw_path)


def transformar_datos_clima(**context) -> str:
    """Convierte el JSON crudo en un CSV limpio con una columna derivada."""
    ti = context["ti"]
    raw_path = Path(ti.xcom_pull(task_ids="extraer_datos_clima"))
    registros = json.loads(raw_path.read_text(encoding="utf-8"))

    filas = []
    for registro in registros:
        clima_actual = registro["current_weather"]
        temperatura = float(clima_actual["temperature"])
        filas.append(
            {
                "ciudad": registro["ciudad"],
                "latitud": registro["latitude"],
                "longitud": registro["longitude"],
                "temperatura_c": temperatura,
                "velocidad_viento_kmh": float(clima_actual["windspeed"]),
                "clasificacion": _clasificar_temperatura(temperatura),
            }
        )

    dataframe = pd.DataFrame(filas)
    processed_path = common.PROCESSED_DIR / f"clima_{context['ds_nodash']}.csv"
    dataframe.to_csv(processed_path, index=False)
    logger.info("Se transformaron %s registros -> %s", len(dataframe), processed_path)
    return str(processed_path)


def cargar_datos_postgres(**context) -> int:
    """Carga el CSV procesado en ``taller.clima_observaciones`` (upsert)."""
    ti = context["ti"]
    processed_path = Path(ti.xcom_pull(task_ids="transformar_datos_clima"))
    dataframe = pd.read_csv(processed_path)
    dataframe["fecha_ejecucion"] = context["ds"]

    columnas = [
        "fecha_ejecucion",
        "ciudad",
        "latitud",
        "longitud",
        "temperatura_c",
        "velocidad_viento_kmh",
        "clasificacion",
    ]
    filas = list(dataframe[columnas].itertuples(index=False, name=None))

    insert_sql = f"""
        INSERT INTO {common.POSTGRES_SCHEMA}.clima_observaciones
            (fecha_ejecucion, ciudad, latitud, longitud, temperatura_c,
             velocidad_viento_kmh, clasificacion)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fecha_ejecucion, ciudad) DO UPDATE SET
            temperatura_c = EXCLUDED.temperatura_c,
            velocidad_viento_kmh = EXCLUDED.velocidad_viento_kmh,
            clasificacion = EXCLUDED.clasificacion,
            cargado_en = now();
    """

    hook = PostgresHook(postgres_conn_id=common.POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, filas)
        conn.commit()
    finally:
        conn.close()

    logger.info("Se cargaron %s filas en %s.clima_observaciones", len(filas), common.POSTGRES_SCHEMA)
    return len(filas)


def generar_resumen(**context) -> None:
    """Consulta la tabla recién cargada y registra un resumen agregado."""
    hook = PostgresHook(postgres_conn_id=common.POSTGRES_CONN_ID)
    query = f"""
        SELECT clasificacion, COUNT(*) AS cantidad, ROUND(AVG(temperatura_c), 1) AS temp_promedio
        FROM {common.POSTGRES_SCHEMA}.clima_observaciones
        WHERE fecha_ejecucion = %s
        GROUP BY clasificacion
        ORDER BY clasificacion;
    """
    registros = hook.get_records(query, parameters=(context["ds"],))

    if not registros:
        logger.warning("No hay observaciones de clima cargadas para la fecha %s", context["ds"])
        return

    logger.info("Resumen de clima para %s:", context["ds"])
    for clasificacion, cantidad, temp_promedio in registros:
        logger.info(" - %-10s | ciudades=%s | temperatura promedio=%s°C", clasificacion, cantidad, temp_promedio)


with DAG(
    dag_id="weather_etl_dag",
    description="ETL de datos climaticos desde Open-Meteo (API REST) hacia PostgreSQL.",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "clima", "api-rest", "taller-03"],
) as dag:

    t1_extraer = PythonOperator(
        task_id="extraer_datos_clima",
        python_callable=extraer_datos_clima,
        doc_md="Consulta la API REST de Open-Meteo para cada ciudad configurada.",
    )

    t2_transformar = PythonOperator(
        task_id="transformar_datos_clima",
        python_callable=transformar_datos_clima,
        doc_md="Limpia y enriquece los datos crudos con pandas (clasificación de temperatura).",
    )

    t3_cargar = PythonOperator(
        task_id="cargar_datos_postgres",
        python_callable=cargar_datos_postgres,
        doc_md="Carga (upsert) los datos procesados en PostgreSQL.",
    )

    t4_resumen = PythonOperator(
        task_id="generar_resumen",
        python_callable=generar_resumen,
        doc_md="Genera un resumen agregado a partir de los datos ya cargados.",
    )

    t1_extraer >> t2_transformar >> t3_cargar >> t4_resumen
