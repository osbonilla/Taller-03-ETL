"""
DAG 4 · data_quality_ingestion_dag
====================================

Propósito
---------
Simula la ingesta de un archivo enviado por un socio externo y aplica
reglas de **calidad de datos** antes de aceptarlo en el pipeline: valida el
esquema, los valores nulos en columnas clave y la presencia de registros
duplicados. Según el resultado, el archivo se mueve a ``data/processed``
(aceptado) o a ``data/quarantine`` (rechazado para revisión manual).

Introduce dos elementos que no aparecen en los DAGs anteriores:

1. **TaskGroup**: las tres validaciones (``validar_esquema``,
   ``validar_valores_nulos``, ``validar_duplicados``) se agrupan en
   ``validaciones_calidad``. Al no depender entre sí, Airflow las ejecuta en
   paralelo, y en la UI se visualizan colapsadas como un único bloque.
2. **Disparo manual/por evento** (``schedule=None``): a diferencia de los
   DAGs 1, 2 y 3 (con calendario fijo), este representa un pipeline que en
   producción se dispararía cuando el archivo efectivamente llega -por
   ejemplo mediante un ``FileSensor`` o una llamada a la API REST de
   Airflow desde el sistema del socio-, no en un horario predefinido.

Flujo de tareas
----------------
    generar_archivo_entrada
        >> [validar_esquema, validar_valores_nulos, validar_duplicados]   (TaskGroup, en paralelo)
        >> consolidar_resultado_validacion
        >> clasificar_y_mover_archivo
        >> generar_reporte_calidad

Una validación que detecta un problema **no** hace fallar su tarea: para un
pipeline de calidad de datos, "el archivo tiene 2 nulos" es un resultado de
negocio válido (se registra y se actúa en consecuencia), no un error de
ejecución. Solo un error de código real haría fallar una tarea aquí.
"""
from __future__ import annotations

import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

import common
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

logger = common.get_logger(__name__)

COLUMNAS_REQUERIDAS = ["id_pedido", "id_cliente", "producto", "cantidad", "precio_unitario"]
COLUMNAS_CLAVE = ["id_pedido", "id_cliente"]
PRODUCTOS = ["Teclado", "Mouse", "Monitor", "Silla", "Escritorio", "Audifonos"]

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def generar_archivo_entrada(**context) -> str:
    """Simula la llegada de un archivo de un socio externo, con problemas de calidad a propósito."""
    common.ensure_data_dirs()
    random.seed(int(context["ds_nodash"]))

    filas = []
    for i in range(40):
        filas.append(
            {
                "id_pedido": f"ORD-{context['ds_nodash']}-{i:03d}",
                "id_cliente": f"CLI-{(i % 15):03d}",
                "producto": random.choice(PRODUCTOS),
                "cantidad": random.randint(1, 5),
                "precio_unitario": round(random.uniform(10, 300), 2),
            }
        )

    # Problemas de calidad deliberados, representativos de un archivo real
    # mal formado que llega de un sistema externo.
    filas[3]["id_cliente"] = None
    filas[7]["id_cliente"] = None
    filas.append(dict(filas[10]))  # registro duplicado (mismo id_pedido)

    dataframe = pd.DataFrame(filas)
    archivo_path = common.INCOMING_DIR / f"pedidos_socio_{context['ds_nodash']}.csv"
    dataframe.to_csv(archivo_path, index=False)
    logger.info("Archivo de entrada generado: %s (%s filas)", archivo_path, len(dataframe))
    return str(archivo_path)


def validar_esquema(**context) -> dict:
    """Verifica que el archivo tenga todas las columnas requeridas."""
    ti = context["ti"]
    archivo_path = Path(ti.xcom_pull(task_ids="generar_archivo_entrada"))
    dataframe = pd.read_csv(archivo_path)

    columnas_faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in dataframe.columns]
    aprobado = len(columnas_faltantes) == 0
    logger.info("Validación de esquema: %s", "OK" if aprobado else f"faltan columnas {columnas_faltantes}")
    return {"paso": "esquema", "aprobado": aprobado, "columnas_faltantes": columnas_faltantes}


def validar_valores_nulos(**context) -> dict:
    """Cuenta valores nulos en las columnas clave del archivo."""
    ti = context["ti"]
    archivo_path = Path(ti.xcom_pull(task_ids="generar_archivo_entrada"))
    dataframe = pd.read_csv(archivo_path)

    nulos = int(dataframe[COLUMNAS_CLAVE].isna().sum().sum())
    aprobado = nulos == 0
    logger.info("Validación de nulos en columnas clave: %s nulo(s) encontrados", nulos)
    return {"paso": "nulos", "aprobado": aprobado, "cantidad": nulos}


def validar_duplicados(**context) -> dict:
    """Cuenta registros con id_pedido duplicado."""
    ti = context["ti"]
    archivo_path = Path(ti.xcom_pull(task_ids="generar_archivo_entrada"))
    dataframe = pd.read_csv(archivo_path)

    duplicados = int(dataframe["id_pedido"].duplicated().sum())
    aprobado = duplicados == 0
    logger.info("Validación de duplicados: %s duplicado(s) encontrados", duplicados)
    return {"paso": "duplicados", "aprobado": aprobado, "cantidad": duplicados}


def consolidar_resultado_validacion(**context) -> dict:
    """Reúne el resultado de las tres validaciones del TaskGroup y decide el veredicto general."""
    ti = context["ti"]
    r_esquema = ti.xcom_pull(task_ids="validaciones_calidad.validar_esquema")
    r_nulos = ti.xcom_pull(task_ids="validaciones_calidad.validar_valores_nulos")
    r_duplicados = ti.xcom_pull(task_ids="validaciones_calidad.validar_duplicados")

    aprobado_general = all([r_esquema["aprobado"], r_nulos["aprobado"], r_duplicados["aprobado"]])
    logger.info("Resultado consolidado de calidad: %s", "APROBADO" if aprobado_general else "RECHAZADO")

    return {
        "aprobado_general": aprobado_general,
        "esquema": r_esquema,
        "nulos": r_nulos,
        "duplicados": r_duplicados,
    }


def clasificar_y_mover_archivo(**context) -> str:
    """Mueve el archivo a 'processed' o 'quarantine' según el veredicto y registra el resultado."""
    ti = context["ti"]
    archivo_path = Path(ti.xcom_pull(task_ids="generar_archivo_entrada"))
    resultado = ti.xcom_pull(task_ids="consolidar_resultado_validacion")

    common.ensure_data_dirs()
    if resultado["aprobado_general"]:
        destino_dir, resultado_final = common.PROCESSED_DIR, "APROBADO"
    else:
        destino_dir, resultado_final = common.QUARANTINE_DIR, "RECHAZADO"

    destino_path = destino_dir / archivo_path.name
    shutil.move(str(archivo_path), str(destino_path))
    logger.info("Archivo '%s' clasificado como %s -> %s", archivo_path.name, resultado_final, destino_path)

    hook = PostgresHook(postgres_conn_id=common.POSTGRES_CONN_ID)
    hook.run(
        f"""
        INSERT INTO {common.POSTGRES_SCHEMA}.calidad_archivos
            (nombre_archivo, esquema_valido, nulos_detectados, duplicados_detectados, resultado_final, destino)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        parameters=(
            archivo_path.name,
            resultado["esquema"]["aprobado"],
            resultado["nulos"]["cantidad"],
            resultado["duplicados"]["cantidad"],
            resultado_final,
            str(destino_path),
        ),
    )
    return str(destino_path)


def generar_reporte_calidad(**context) -> None:
    """Registra un resumen legible del resultado de calidad en el log de la tarea."""
    ti = context["ti"]
    resultado = ti.xcom_pull(task_ids="consolidar_resultado_validacion")
    destino_path = ti.xcom_pull(task_ids="clasificar_y_mover_archivo")

    logger.info("=== Reporte de calidad de datos (%s) ===", context["ds"])
    logger.info("Esquema válido:        %s", resultado["esquema"]["aprobado"])
    logger.info("Nulos en clave:        %s", resultado["nulos"]["cantidad"])
    logger.info("Duplicados:            %s", resultado["duplicados"]["cantidad"])
    logger.info("Resultado final:       %s", "APROBADO" if resultado["aprobado_general"] else "RECHAZADO")
    logger.info("Archivo final:         %s", destino_path)


with DAG(
    dag_id="data_quality_ingestion_dag",
    description="Ingesta de archivos de un socio externo con validaciones de calidad (TaskGroup) y cuarentena.",
    default_args=default_args,
    schedule=None,  # disparo manual / por evento (llegada de archivo), no por calendario
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "task-group", "ingesta", "taller-03"],
) as dag:

    t1_generar = PythonOperator(
        task_id="generar_archivo_entrada",
        python_callable=generar_archivo_entrada,
        doc_md="Simula la llegada de un archivo de pedidos de un socio externo.",
    )

    with TaskGroup(group_id="validaciones_calidad") as grupo_validaciones:
        t2a_esquema = PythonOperator(
            task_id="validar_esquema",
            python_callable=validar_esquema,
            doc_md="Verifica que existan todas las columnas requeridas.",
        )
        t2b_nulos = PythonOperator(
            task_id="validar_valores_nulos",
            python_callable=validar_valores_nulos,
            doc_md="Cuenta valores nulos en las columnas clave (id_pedido, id_cliente).",
        )
        t2c_duplicados = PythonOperator(
            task_id="validar_duplicados",
            python_callable=validar_duplicados,
            doc_md="Cuenta registros con id_pedido duplicado.",
        )
        # Las tres tareas no dependen entre sí: Airflow las ejecuta en paralelo.

    t3_consolidar = PythonOperator(
        task_id="consolidar_resultado_validacion",
        python_callable=consolidar_resultado_validacion,
        doc_md="Reúne el resultado de las tres validaciones y decide el veredicto general.",
    )

    t4_clasificar = PythonOperator(
        task_id="clasificar_y_mover_archivo",
        python_callable=clasificar_y_mover_archivo,
        doc_md="Mueve el archivo a processed/ o quarantine/ según el veredicto.",
    )

    t5_reporte = PythonOperator(
        task_id="generar_reporte_calidad",
        python_callable=generar_reporte_calidad,
        doc_md="Registra un resumen legible del resultado de calidad.",
    )

    t1_generar >> grupo_validaciones >> t3_consolidar >> t4_clasificar >> t5_reporte
