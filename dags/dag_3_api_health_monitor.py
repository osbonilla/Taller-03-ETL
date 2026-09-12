"""
DAG 3 · api_health_monitor_dag
===============================

Propósito
---------
Monitorea periódicamente la disponibilidad de un conjunto de endpoints HTTP
y decide, en función del resultado, si registrar un estado normal o generar
una alerta. Introduce el concepto de **ramificación condicional** en Airflow
(``BranchPythonOperator``) y de **trigger rules**, dos mecanismos que el DAG
1 y el DAG 2 -ambos estrictamente lineales- no requieren.

Flujo de tareas
----------------
                              -> generar_alerta ------\\
    verificar_endpoints -> evaluar_estado_general                    -> generar_reporte_monitoreo
                              -> registrar_estado_ok --/

- ``verificar_endpoints``: realiza una petición HTTP a cada endpoint
  configurado y registra código de estado y latencia. No lanza una
  excepción si un endpoint no responde: para una tarea de monitoreo,
  "el servicio está caído" es un resultado válido del negocio, no un error
  de ejecución de la tarea.
- ``evaluar_estado_general`` (**BranchPythonOperator**): decide, según los
  resultados anteriores, cuál de las dos ramas siguientes ejecutar. Solo una
  de las dos se ejecuta; Airflow marca la otra como ``skipped``.
- ``generar_alerta`` / ``registrar_estado_ok``: ramas mutuamente excluyentes.
- ``generar_reporte_monitoreo``: se ejecuta siempre, sin importar cuál rama
  corrió, gracias a ``trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS``
  (se ejecuta si ninguna de sus tareas predecesoras falló y al menos una
  tuvo éxito -que es justamente lo que ocurre con una rama ejecutada y la
  otra "skipped"-). Consolida los resultados y los guarda en PostgreSQL.

Los endpoints a monitorear se configuran mediante la variable de entorno
``TALLER_MONITOR_ENDPOINTS`` (URLs separadas por coma). Se incluye por
defecto ``httpbin.org/status/500``, un endpoint público que siempre
responde con error 500, para que la rama de alerta pueda observarse en la
UI sin depender de que un servicio real esté caído en el momento de la demo.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import requests

import common
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule

logger = common.get_logger(__name__)

DEFAULT_ENDPOINTS = "https://api.github.com,https://pypi.org,https://httpbin.org/status/500"
ENDPOINTS = [url.strip() for url in os.environ.get("TALLER_MONITOR_ENDPOINTS", DEFAULT_ENDPOINTS).split(",") if url.strip()]

TASK_ID_ALERTA = "generar_alerta"
TASK_ID_OK = "registrar_estado_ok"

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def verificar_endpoints(**context) -> list[dict]:
    """Consulta cada endpoint configurado y registra su estado y latencia."""
    resultados = []
    headers = {"User-Agent": "taller-03-etl-health-monitor/1.0"}
    for url in ENDPOINTS:
        inicio = time.perf_counter()
        try:
            response = requests.get(url, headers=headers, timeout=10)
            latencia_ms = round((time.perf_counter() - inicio) * 1000, 1)
            saludable = response.status_code < 400
            resultados.append(
                {"url": url, "codigo_estado": response.status_code, "latencia_ms": latencia_ms, "saludable": saludable}
            )
            logger.info("%s -> HTTP %s (%.1f ms) %s", url, response.status_code, latencia_ms, "OK" if saludable else "DEGRADADO")
        except requests.RequestException as exc:
            latencia_ms = round((time.perf_counter() - inicio) * 1000, 1)
            resultados.append({"url": url, "codigo_estado": None, "latencia_ms": latencia_ms, "saludable": False})
            logger.warning("%s -> sin respuesta (%s)", url, exc)
    return resultados


def evaluar_estado_general(**context) -> str:
    """Decide la siguiente tarea a ejecutar según el resultado del chequeo."""
    ti = context["ti"]
    resultados = ti.xcom_pull(task_ids="verificar_endpoints")
    hay_incidentes = any(not r["saludable"] for r in resultados)

    if hay_incidentes:
        logger.info("Se detectaron endpoints no saludables -> rama '%s'", TASK_ID_ALERTA)
        return TASK_ID_ALERTA

    logger.info("Todos los endpoints responden correctamente -> rama '%s'", TASK_ID_OK)
    return TASK_ID_OK


def generar_alerta(**context) -> None:
    """Simula el envío de una alerta (registra el detalle en el log y en un archivo)."""
    ti = context["ti"]
    resultados = ti.xcom_pull(task_ids="verificar_endpoints")
    no_saludables = [r for r in resultados if not r["saludable"]]

    common.ensure_data_dirs()
    log_path = common.PROCESSED_DIR / "alertas_monitoreo.log"
    with log_path.open("a", encoding="utf-8") as archivo:
        for r in no_saludables:
            linea = f"{context['ts']} | ALERTA | {r['url']} | codigo={r['codigo_estado']} | latencia_ms={r['latencia_ms']}\n"
            archivo.write(linea)
            logger.warning("ALERTA: %s respondio con codigo=%s", r["url"], r["codigo_estado"])

    logger.info("Se registraron %s alertas en %s", len(no_saludables), log_path)


def registrar_estado_ok(**context) -> None:
    """Registra que, en este chequeo, todos los endpoints están saludables."""
    logger.info("Chequeo OK: los %s endpoints monitoreados respondieron correctamente.", len(ENDPOINTS))


def generar_reporte_monitoreo(**context) -> None:
    """Consolida el resultado del chequeo (haya habido alerta o no) en PostgreSQL."""
    ti = context["ti"]
    resultados = ti.xcom_pull(task_ids="verificar_endpoints")

    hook = PostgresHook(postgres_conn_id=common.POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {common.POSTGRES_SCHEMA}.monitoreo_endpoints
                    (endpoint, codigo_estado, latencia_ms, saludable)
                VALUES (%s, %s, %s, %s);
                """,
                [(r["url"], r["codigo_estado"], r["latencia_ms"], r["saludable"]) for r in resultados],
            )
        conn.commit()
    finally:
        conn.close()

    saludables = sum(1 for r in resultados if r["saludable"])
    logger.info("Reporte de monitoreo guardado: %s/%s endpoints saludables.", saludables, len(resultados))


with DAG(
    dag_id="api_health_monitor_dag",
    description="Monitoreo de endpoints HTTP con ramificacion condicional (alerta vs. estado OK).",
    default_args=default_args,
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["monitoreo", "branching", "api-rest", "taller-03"],
) as dag:

    t1_verificar = PythonOperator(
        task_id="verificar_endpoints",
        python_callable=verificar_endpoints,
        doc_md="Consulta cada endpoint configurado y registra código de estado y latencia.",
    )

    t2_evaluar = BranchPythonOperator(
        task_id="evaluar_estado_general",
        python_callable=evaluar_estado_general,
        doc_md="Decide si continuar por la rama de alerta o la rama de estado OK.",
    )

    t3a_alerta = PythonOperator(
        task_id=TASK_ID_ALERTA,
        python_callable=generar_alerta,
        doc_md="Rama ejecutada solo si algún endpoint no respondió correctamente.",
    )

    t3b_ok = PythonOperator(
        task_id=TASK_ID_OK,
        python_callable=registrar_estado_ok,
        doc_md="Rama ejecutada solo si todos los endpoints respondieron correctamente.",
    )

    t4_reporte = PythonOperator(
        task_id="generar_reporte_monitoreo",
        python_callable=generar_reporte_monitoreo,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Se ejecuta siempre (haya ido por la rama de alerta o la de OK) y persiste el resultado.",
    )

    t1_verificar >> t2_evaluar >> [t3a_alerta, t3b_ok] >> t4_reporte
