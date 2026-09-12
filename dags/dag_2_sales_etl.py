"""
DAG 2 · sales_etl_dag
=====================

Propósito
---------
Pipeline ETL de ventas que simula la extracción de un archivo diario de
pedidos (como llegaría de un sistema transaccional externo), lo limpia con
pandas, calcula agregaciones de negocio y carga tanto el detalle como el
resumen en PostgreSQL. A diferencia del DAG 1 (lineal y simple), este DAG
hace un uso explícito de XCom para pasar entre tareas no solo rutas de
archivo sino también métricas (filas originales, filas limpias, filas
cargadas), que se consolidan en la última tarea de validación.

Flujo de tareas
----------------
    generar_datos_ventas_diarias
        >> limpiar_datos_ventas
        >> calcular_metricas_ventas
        >> cargar_ventas_postgres
        >> validar_carga_ventas

- ``generar_datos_ventas_diarias``: simula la extracción de un archivo de
  pedidos del día (incluye a propósito nulos, duplicados y montos inválidos,
  como ocurriría con datos reales de origen).
- ``limpiar_datos_ventas``: aplica reglas de limpieza con pandas
  (elimina duplicados, filas con campos clave nulos y montos no positivos).
- ``calcular_metricas_ventas``: agrupa por categoría y región para obtener
  el total vendido y la cantidad de pedidos.
- ``cargar_ventas_postgres``: inserta el detalle limpio y el resumen
  agregado en PostgreSQL.
- ``validar_carga_ventas``: compara el número de filas que se intentó
  cargar contra el número de filas realmente presentes en la base de datos
  para la fecha de ejecución (control de calidad de la carga).
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

import common
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

logger = common.get_logger(__name__)

CATEGORIAS = ["Electronica", "Ropa", "Hogar", "Alimentos", "Juguetes"]
REGIONES = ["Norte", "Sur", "Centro", "Este", "Oeste"]

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def generar_datos_ventas_diarias(**context) -> str:
    """Simula la extracción de un archivo de pedidos con datos "sucios".

    En un escenario real, esta tarea sería reemplazada por la descarga de un
    archivo desde un SFTP, un bucket de object storage o una API del sistema
    de ventas. Se usa una semilla derivada de la fecha de ejecución para que
    el mismo ``ds`` siempre genere el mismo dataset (reproducibilidad).
    """
    common.ensure_data_dirs()
    random.seed(int(context["ds_nodash"]))

    filas = []
    n_pedidos = 300
    for i in range(n_pedidos):
        filas.append(
            {
                "id_pedido": f"PED-{context['ds_nodash']}-{i:04d}",
                "categoria": random.choice(CATEGORIAS),
                "region": random.choice(REGIONES),
                "monto": round(random.uniform(5, 500), 2),
            }
        )

    # Se inyectan deliberadamente algunos problemas de calidad típicos de
    # datos de origen "reales" para que la tarea de limpieza tenga trabajo
    # real que hacer (y quede documentado en el log cuánto se descartó).
    for i in range(8):
        filas.append({"id_pedido": None, "categoria": random.choice(CATEGORIAS), "region": "Norte", "monto": 42.0})
    for i in range(5):
        filas.append({"id_pedido": filas[i]["id_pedido"], "categoria": filas[i]["categoria"], "region": filas[i]["region"], "monto": filas[i]["monto"]})
    for i in range(4):
        filas.append({"id_pedido": f"PED-{context['ds_nodash']}-ERR{i}", "categoria": random.choice(CATEGORIAS), "region": "Sur", "monto": -15.0})

    dataframe = pd.DataFrame(filas)
    raw_path = common.RAW_DIR / f"ventas_{context['ds_nodash']}.csv"
    dataframe.to_csv(raw_path, index=False)
    logger.info("Archivo de ventas generado: %s (%s filas, incluye datos sucios de prueba)", raw_path, len(dataframe))
    return str(raw_path)


def limpiar_datos_ventas(**context) -> dict:
    """Limpia el archivo de ventas: sin duplicados, sin nulos clave, montos válidos."""
    ti = context["ti"]
    raw_path = Path(ti.xcom_pull(task_ids="generar_datos_ventas_diarias"))
    dataframe = pd.read_csv(raw_path)
    filas_originales = len(dataframe)

    dataframe = dataframe.dropna(subset=["id_pedido"])
    dataframe = dataframe.drop_duplicates(subset=["id_pedido"], keep="first")
    dataframe = dataframe[dataframe["monto"] > 0]

    filas_limpias = len(dataframe)
    processed_path = common.PROCESSED_DIR / f"ventas_limpias_{context['ds_nodash']}.csv"
    dataframe.to_csv(processed_path, index=False)

    logger.info(
        "Limpieza completa: %s filas originales -> %s filas válidas (%s descartadas)",
        filas_originales,
        filas_limpias,
        filas_originales - filas_limpias,
    )
    return {"path": str(processed_path), "filas_originales": filas_originales, "filas_limpias": filas_limpias}


def calcular_metricas_ventas(**context) -> str:
    """Agrega el detalle limpio por categoría y región."""
    ti = context["ti"]
    info_limpieza = ti.xcom_pull(task_ids="limpiar_datos_ventas")
    dataframe = pd.read_csv(info_limpieza["path"])

    resumen = (
        dataframe.groupby(["categoria", "region"], as_index=False)
        .agg(total_ventas=("monto", "sum"), cantidad_pedidos=("id_pedido", "count"))
        .round({"total_ventas": 2})
    )

    resumen_path = common.PROCESSED_DIR / f"ventas_resumen_{context['ds_nodash']}.csv"
    resumen.to_csv(resumen_path, index=False)
    logger.info("Se calcularon métricas para %s combinaciones categoría/región -> %s", len(resumen), resumen_path)
    return str(resumen_path)


def cargar_ventas_postgres(**context) -> dict:
    """Carga el detalle limpio y el resumen agregado en PostgreSQL."""
    ti = context["ti"]
    info_limpieza = ti.xcom_pull(task_ids="limpiar_datos_ventas")
    resumen_path = ti.xcom_pull(task_ids="calcular_metricas_ventas")
    ds = context["ds"]

    detalle = pd.read_csv(info_limpieza["path"])
    detalle["fecha_ejecucion"] = ds
    resumen = pd.read_csv(resumen_path)
    resumen["fecha_ejecucion"] = ds

    hook = PostgresHook(postgres_conn_id=common.POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {common.POSTGRES_SCHEMA}.ventas_detalle
                    (fecha_ejecucion, id_pedido, categoria, region, monto)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fecha_ejecucion, id_pedido) DO NOTHING;
                """,
                list(detalle[["fecha_ejecucion", "id_pedido", "categoria", "region", "monto"]].itertuples(index=False, name=None)),
            )
            cursor.executemany(
                f"""
                INSERT INTO {common.POSTGRES_SCHEMA}.ventas_resumen
                    (fecha_ejecucion, categoria, region, total_ventas, cantidad_pedidos)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fecha_ejecucion, categoria, region) DO UPDATE SET
                    total_ventas = EXCLUDED.total_ventas,
                    cantidad_pedidos = EXCLUDED.cantidad_pedidos,
                    cargado_en = now();
                """,
                list(resumen[["fecha_ejecucion", "categoria", "region", "total_ventas", "cantidad_pedidos"]].itertuples(index=False, name=None)),
            )
        conn.commit()
    finally:
        conn.close()

    logger.info("Cargadas %s filas de detalle y %s filas de resumen", len(detalle), len(resumen))
    return {"filas_detalle_cargadas": len(detalle), "filas_resumen_cargadas": len(resumen)}


def validar_carga_ventas(**context) -> None:
    """Compara lo que se intentó cargar contra lo que realmente hay en la base."""
    ti = context["ti"]
    carga_info = ti.xcom_pull(task_ids="cargar_ventas_postgres")
    ds = context["ds"]

    hook = PostgresHook(postgres_conn_id=common.POSTGRES_CONN_ID)
    (conteo_detalle,) = hook.get_first(
        f"SELECT COUNT(*) FROM {common.POSTGRES_SCHEMA}.ventas_detalle WHERE fecha_ejecucion = %s;",
        parameters=(ds,),
    )

    esperado = carga_info["filas_detalle_cargadas"]
    if conteo_detalle < esperado:
        logger.warning(
            "Posible pérdida de datos: se intentaron cargar %s filas de detalle pero solo hay %s en la base "
            "(puede deberse a ejecuciones previas del mismo día con id_pedido repetido).",
            esperado,
            conteo_detalle,
        )
    else:
        logger.info("Validación OK: %s filas de detalle confirmadas en la base para %s", conteo_detalle, ds)


with DAG(
    dag_id="sales_etl_dag",
    description="ETL de ventas: generacion, limpieza con pandas, agregaciones y carga a PostgreSQL.",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "ventas", "pandas", "taller-03"],
) as dag:

    t1_generar = PythonOperator(
        task_id="generar_datos_ventas_diarias",
        python_callable=generar_datos_ventas_diarias,
        doc_md="Simula la llegada de un archivo diario de pedidos desde el sistema de ventas.",
    )

    t2_limpiar = PythonOperator(
        task_id="limpiar_datos_ventas",
        python_callable=limpiar_datos_ventas,
        doc_md="Elimina duplicados, nulos en campos clave y montos inválidos.",
    )

    t3_metricas = PythonOperator(
        task_id="calcular_metricas_ventas",
        python_callable=calcular_metricas_ventas,
        doc_md="Agrega el total vendido y la cantidad de pedidos por categoría y región.",
    )

    t4_cargar = PythonOperator(
        task_id="cargar_ventas_postgres",
        python_callable=cargar_ventas_postgres,
        doc_md="Carga el detalle limpio y el resumen agregado en PostgreSQL.",
    )

    t5_validar = PythonOperator(
        task_id="validar_carga_ventas",
        python_callable=validar_carga_ventas,
        doc_md="Compara las filas cargadas contra las filas realmente presentes en la base (control de calidad).",
    )

    t1_generar >> t2_limpiar >> t3_metricas >> t4_cargar >> t5_validar
