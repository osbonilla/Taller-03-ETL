"""Pruebas del taller: lógica pura de negocio y estructura de los 5 DAGs.

Se dividen en dos grupos:

1. Lógica pura (no requiere Airflow ni PostgreSQL en ejecución): funciones
   de clasificación/transformación que pueden probarse de forma aislada.
2. Estructura de los DAGs: verifica que cada DAG exponga el dag_id esperado,
   al menos tres tareas (requisito del taller) y las dependencias básicas,
   sin necesidad de ejecutar ninguna tarea real. Airflow ya valida por sí
   solo que no haya ciclos al construir el DAG (lanzaría una excepción al
   importarlo), así que si el import no falla, el grafo es válido.

Ejecución:
    pip install -r requirements-dev.txt --constraint <constraints-url>
    pytest tests/ -v
"""
import dag_1_weather_etl as weather_mod
import dag_2_sales_etl as sales_mod
import dag_3_api_health_monitor as monitor_mod
import dag_4_data_quality_ingestion as quality_mod
import dag_5_backup_cleanup as backup_mod

# ---------------------------------------------------------------------------
# 1. Lógica pura
# ---------------------------------------------------------------------------


def test_clasificar_temperatura_frio():
    assert weather_mod._clasificar_temperatura(-5) == "frio"
    assert weather_mod._clasificar_temperatura(9.9) == "frio"


def test_clasificar_temperatura_templado():
    assert weather_mod._clasificar_temperatura(10) == "templado"
    assert weather_mod._clasificar_temperatura(21.9) == "templado"


def test_clasificar_temperatura_calido():
    assert weather_mod._clasificar_temperatura(22) == "calido"
    assert weather_mod._clasificar_temperatura(35) == "calido"


# ---------------------------------------------------------------------------
# 2. Estructura de los DAGs (>= 3 tareas, dag_id correcto, tags presentes)
# ---------------------------------------------------------------------------

DAGS_ESPERADOS = {
    "weather_etl_dag": (weather_mod, {"extraer_datos_clima", "transformar_datos_clima", "cargar_datos_postgres", "generar_resumen"}),
    "sales_etl_dag": (sales_mod, {"generar_datos_ventas_diarias", "limpiar_datos_ventas", "calcular_metricas_ventas", "cargar_ventas_postgres", "validar_carga_ventas"}),
    "api_health_monitor_dag": (monitor_mod, {"verificar_endpoints", "evaluar_estado_general", "generar_alerta", "registrar_estado_ok", "generar_reporte_monitoreo"}),
    "data_quality_ingestion_dag": (
        quality_mod,
        {
            "generar_archivo_entrada",
            "validaciones_calidad.validar_esquema",
            "validaciones_calidad.validar_valores_nulos",
            "validaciones_calidad.validar_duplicados",
            "consolidar_resultado_validacion",
            "clasificar_y_mover_archivo",
            "generar_reporte_calidad",
        },
    ),
    "backup_cleanup_dag": (backup_mod, {"crear_backup_tablas", "verificar_integridad_backup", "limpiar_backups_antiguos", "verificar_espacio_almacenamiento", "reportar_estado_mantenimiento"}),
}


def test_los_cinco_dags_tienen_al_menos_tres_tareas():
    for dag_id, (modulo, _tareas_esperadas) in DAGS_ESPERADOS.items():
        dag = modulo.dag
        assert dag.dag_id == dag_id
        assert len(dag.task_ids) >= 3, f"{dag_id} tiene menos de 3 tareas"


def test_los_cinco_dags_tienen_las_tareas_esperadas():
    for dag_id, (modulo, tareas_esperadas) in DAGS_ESPERADOS.items():
        dag = modulo.dag
        assert set(dag.task_ids) == tareas_esperadas, f"Tareas inesperadas en {dag_id}: {set(dag.task_ids)}"


def test_todos_los_dags_tienen_tag_taller_03():
    for _dag_id, (modulo, _tareas) in DAGS_ESPERADOS.items():
        assert "taller-03" in modulo.dag.tags


def test_dag3_tiene_ramificacion_condicional():
    dag = monitor_mod.dag
    tarea_branch = dag.get_task("evaluar_estado_general")
    downstream = set(tarea_branch.downstream_task_ids)
    assert downstream == {"generar_alerta", "registrar_estado_ok"}


def test_dag5_tiene_convergencia_en_paralelo_fan_in():
    dag = backup_mod.dag
    tarea_convergencia = dag.get_task("verificar_espacio_almacenamiento")
    upstream = set(tarea_convergencia.upstream_task_ids)
    assert upstream == {"verificar_integridad_backup", "limpiar_backups_antiguos"}
