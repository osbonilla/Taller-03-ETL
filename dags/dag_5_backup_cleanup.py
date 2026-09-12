"""
DAG 5 · backup_cleanup_dag
===========================

Propósito
---------
Tarea de mantenimiento que respalda las tablas clave del esquema
``taller`` en PostgreSQL, verifica la integridad del archivo generado y
aplica una política de retención eliminando backups antiguos. Es el único
de los cinco DAGs que ejecuta explícitamente **tareas en paralelo con
convergencia (fan-in)**, y el único que combina ``PythonOperator`` con
``BashOperator`` y con una **Airflow Variable**.

Flujo de tareas
----------------
    crear_backup_tablas >> verificar_integridad_backup  ---\\
    limpiar_backups_antiguos (independiente, en paralelo) ---+--> verificar_espacio_almacenamiento >> reportar_estado_mantenimiento

- ``crear_backup_tablas``: exporta las tablas configuradas a CSV y las
  empaqueta en un ``.tar.gz`` con timestamp.
- ``verificar_integridad_backup``: confirma que el archivo existe, pesa más
  de 0 bytes y que el ``.tar.gz`` puede abrirse y contiene los archivos
  esperados. A diferencia de las validaciones del DAG 4, aquí un backup
  corrupto sí hace fallar la tarea (``raise``): no es un resultado de
  negocio válido, es una falla operativa real que debe frenar el pipeline.
- ``limpiar_backups_antiguos``: no depende de las dos tareas anteriores
  -puede ejecutarse en paralelo- y elimina backups más viejos que
  ``taller_backup_retention_days`` (Airflow Variable, con valor por defecto
  de 7 días si no se configura explícitamente).
- ``verificar_espacio_almacenamiento`` (**BashOperator**): espera a que
  terminen tanto la verificación de integridad como la limpieza (fan-in) y
  reporta con ``du -sh`` cuánto espacio ocupa la carpeta de backups.
- ``reportar_estado_mantenimiento``: consolida todo en un registro de
  ``taller.mantenimiento_backups``.
"""
from __future__ import annotations

import shutil
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import common
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

logger = common.get_logger(__name__)

TABLAS_A_RESPALDAR = ["clima_observaciones", "ventas_resumen"]

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def crear_backup_tablas(**context) -> dict:
    """Exporta las tablas configuradas a CSV y las empaqueta en un .tar.gz."""
    common.ensure_data_dirs()
    hook = PostgresHook(postgres_conn_id=common.POSTGRES_CONN_ID)

    carpeta_temporal = common.BACKUPS_DIR / f"_tmp_{context['ts_nodash']}"
    carpeta_temporal.mkdir(parents=True, exist_ok=True)

    archivos_generados = []
    for tabla in TABLAS_A_RESPALDAR:
        dataframe = hook.get_pandas_df(f"SELECT * FROM {common.POSTGRES_SCHEMA}.{tabla};")
        csv_path = carpeta_temporal / f"{tabla}.csv"
        dataframe.to_csv(csv_path, index=False)
        archivos_generados.append(csv_path)
        logger.info("Tabla %s respaldada (%s filas) -> %s", tabla, len(dataframe), csv_path)

    backup_comprimido = common.BACKUPS_DIR / f"backup_{context['ts_nodash']}.tar.gz"
    with tarfile.open(backup_comprimido, "w:gz") as tar:
        for archivo in archivos_generados:
            tar.add(archivo, arcname=archivo.name)
    shutil.rmtree(carpeta_temporal)

    tamano_bytes = backup_comprimido.stat().st_size
    logger.info("Backup comprimido creado: %s (%s bytes)", backup_comprimido, tamano_bytes)
    return {"archivo_backup": str(backup_comprimido), "tamano_bytes": tamano_bytes}


def verificar_integridad_backup(**context) -> bool:
    """Confirma que el .tar.gz generado existe, pesa >0 bytes y es legible."""
    ti = context["ti"]
    info_backup = ti.xcom_pull(task_ids="crear_backup_tablas")
    backup_path = Path(info_backup["archivo_backup"])

    existe = backup_path.exists()
    tamano_valido = existe and backup_path.stat().st_size > 0
    contenido_legible = False
    if tamano_valido:
        try:
            with tarfile.open(backup_path, "r:gz") as tar:
                contenido_legible = len(tar.getnames()) == len(TABLAS_A_RESPALDAR)
        except tarfile.TarError:
            contenido_legible = False

    integridad_ok = existe and tamano_valido and contenido_legible
    logger.info(
        "Verificación de integridad -> existe=%s, tamaño_valido=%s, contenido_legible=%s",
        existe,
        tamano_valido,
        contenido_legible,
    )

    if not integridad_ok:
        # Un backup corrupto es una falla operativa real: se detiene el pipeline.
        raise ValueError(f"El backup {backup_path} no pasó la verificación de integridad.")
    return integridad_ok


def limpiar_backups_antiguos(**context) -> int:
    """Elimina backups más antiguos que la política de retención configurada."""
    common.ensure_data_dirs()
    retention_days = int(Variable.get("taller_backup_retention_days", default=7))
    limite = datetime.now(timezone.utc) - timedelta(days=retention_days)

    eliminados = 0
    for archivo in common.BACKUPS_DIR.glob("backup_*.tar.gz"):
        modificado = datetime.fromtimestamp(archivo.stat().st_mtime, tz=timezone.utc)
        if modificado < limite:
            archivo.unlink()
            eliminados += 1
            logger.info("Backup antiguo eliminado (retención=%s días): %s", retention_days, archivo.name)

    logger.info("Limpieza de backups completa: %s archivo(s) eliminado(s) (retención=%s días)", eliminados, retention_days)
    return eliminados


def reportar_estado_mantenimiento(**context) -> None:
    """Consolida el resultado del backup, la integridad y la limpieza en PostgreSQL."""
    ti = context["ti"]
    info_backup = ti.xcom_pull(task_ids="crear_backup_tablas")
    integridad_ok = ti.xcom_pull(task_ids="verificar_integridad_backup")
    backups_eliminados = ti.xcom_pull(task_ids="limpiar_backups_antiguos")

    hook = PostgresHook(postgres_conn_id=common.POSTGRES_CONN_ID)
    hook.run(
        f"""
        INSERT INTO {common.POSTGRES_SCHEMA}.mantenimiento_backups
            (archivo_backup, tamano_bytes, integridad_ok, backups_eliminados)
        VALUES (%s, %s, %s, %s);
        """,
        parameters=(
            info_backup["archivo_backup"],
            info_backup["tamano_bytes"],
            bool(integridad_ok),
            int(backups_eliminados),
        ),
    )

    logger.info("=== Reporte de mantenimiento (%s) ===", context["ds"])
    logger.info("Backup creado:               %s (%s bytes)", info_backup["archivo_backup"], info_backup["tamano_bytes"])
    logger.info("Integridad verificada:       %s", integridad_ok)
    logger.info("Backups antiguos eliminados: %s", backups_eliminados)


with DAG(
    dag_id="backup_cleanup_dag",
    description="Backup de tablas clave, verificacion de integridad y limpieza de backups antiguos (fan-in en paralelo).",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["mantenimiento", "backup", "paralelismo", "taller-03"],
) as dag:

    t1_backup = PythonOperator(
        task_id="crear_backup_tablas",
        python_callable=crear_backup_tablas,
        doc_md="Exporta las tablas configuradas a CSV y las empaqueta en un .tar.gz.",
    )

    t2_verificar = PythonOperator(
        task_id="verificar_integridad_backup",
        python_callable=verificar_integridad_backup,
        doc_md="Confirma que el backup generado existe, pesa >0 bytes y es un .tar.gz válido.",
    )

    t3_limpiar = PythonOperator(
        task_id="limpiar_backups_antiguos",
        python_callable=limpiar_backups_antiguos,
        doc_md="Elimina backups más antiguos que la política de retención. No depende del backup del día -corre en paralelo-.",
    )

    t4_espacio = BashOperator(
        task_id="verificar_espacio_almacenamiento",
        bash_command=f'echo "Uso de almacenamiento en la carpeta de backups:" && du -sh "{common.BACKUPS_DIR}" 2>/dev/null || echo "(carpeta de backups vacia o inexistente)"',
        doc_md="Reporta el espacio en disco usado por la carpeta de backups (fan-in: espera integridad + limpieza).",
    )

    t5_reporte = PythonOperator(
        task_id="reportar_estado_mantenimiento",
        python_callable=reportar_estado_mantenimiento,
        doc_md="Consolida backup, integridad y limpieza en PostgreSQL.",
    )

    t1_backup >> t2_verificar
    [t2_verificar, t3_limpiar] >> t4_espacio >> t5_reporte
