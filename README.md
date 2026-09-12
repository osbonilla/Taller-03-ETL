# Taller 03 — ETL: 5 DAGs en Apache Airflow

**Evelyn Nathaly Bermeo Granda**

**Oldrin Santiago Bonilla Cáceres**

Contiene cinco pipelines de datos independientes, cada uno con al menos tres tareas
interconectadas en una secuencia lógica, orquestados con Apache Airflow 3.3.1
sobre Docker Compose, con PostgreSQL como almacenamiento.

## Índice

- [Arquitectura](#arquitectura)
- [Stack tecnológico](#stack-tecnológico)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Los 5 DAGs](#los-5-dags)
- [Cómo ejecutar el proyecto](#cómo-ejecutar-el-proyecto)
- [Cómo probar cada DAG desde la UI](#cómo-probar-cada-dag-desde-la-ui)
- [Validación automática incluida](#validación-automática-incluida)
- [Errores comunes](#errores-comunes)

---

## Arquitectura

```text
   Fuentes de datos                 Ingestion / Transform            Almacenamiento
 (API REST, archivos       ─▶     (Python + pandas, dentro    ─▶    PostgreSQL
  generados, Postgres)              de cada task de Airflow)         (esquema "taller")
                                            │
                                            ▼
                                  Apache Airflow (orquestación)
                                  scheduler + dag-processor +
                                     api-server + triggerer
                                            │
                                            ▼
                              Logs, historial de ejecución y
                                UI web (localhost:8080)
```

Cada DAG cubre un tramo distinto de ese flujo general (extracción vía API,
limpieza con pandas, validación de calidad, carga a PostgreSQL, mantenimiento),
de modo que entre los cinco se cubre el ciclo completo que pide el taller.

PostgreSQL cumple **dos roles** dentro del mismo contenedor: guarda los
metadatos internos de Airflow (base `airflow`) y, en un esquema separado
(`taller`), los datos de negocio que producen los DAGs. Levantar un segundo
motor de base de datos solo para el taller habría sido complejidad
innecesaria; separar por esquema logra el mismo aislamiento sin ese costo.

## Stack tecnológico

| Tecnología | Versión | Propósito |
|---|---|---|
| Apache Airflow | 3.3.1 | Orquestación de los 5 pipelines |
| PostgreSQL | 16 | Metadatos de Airflow + datos de negocio (esquema `taller`) |
| Python | 3.12 | Lenguaje de los DAGs y la lógica de negocio |
| pandas | 3.0.5 | Limpieza, transformación y agregación de datos |
| requests | 2.34.2 | Consumo de APIs REST (clima, monitoreo) |
| Docker / Docker Compose | — | Empaquetado y orquestación de los servicios |
| pytest | 8.3.4 | Pruebas automáticas de lógica y estructura de los DAGs |

Se descartó agregar herramientas adicionales (Great Expectations, DuckDB,
Celery/Redis, etc.): con 5 DAGs y una carga de ejecución pequeña, `LocalExecutor`
ya ofrece ejecución en paralelo (ver DAG 5) y las validaciones de calidad se
resuelven con pandas puro, sin sumar dependencias que no aportan al alcance
de este taller.

## Estructura del repositorio

```text
Taller-03-ETL/
├── README.md                     Este archivo
├── docker-compose.yml            Orquesta Postgres + los 4 componentes de Airflow
├── Dockerfile                    Imagen de Airflow + dependencias del proyecto
├── requirements.txt              Dependencias que corren DENTRO del contenedor
├── requirements-dev.txt          Dependencias para desarrollo/pruebas locales
├── .env.example                  Variables necesarias para docker-compose.yml
├── .gitignore
│
├── dags/
│   ├── common.py                 Configuración compartida (rutas, conexión Postgres)
│   ├── dag_1_weather_etl.py
│   ├── dag_2_sales_etl.py
│   ├── dag_3_api_health_monitor.py
│   ├── dag_4_data_quality_ingestion.py
│   └── dag_5_backup_cleanup.py
│
├── sql/
│   └── init.sql                  Esquema "taller" y sus tablas (auto-aplicado por Postgres)
│
├── data/                         Carpeta montada en /opt/airflow/data (raw, processed, quarantine, backups)
├── tests/                        Pruebas automáticas (pytest) de lógica y estructura de los DAGs
├── scripts/                      Scripts auxiliares de verificación
└── docs/
    └── capturas/                 Carpeta para las capturas de pantalla de la ejecución (ver PDF)
```

> **Nota sobre la estructura:** la propuesta inicial de este taller consideraba
> una carpeta `src/` con subcarpetas `ingestion/transformation/validation/database/`
> para un proyecto más amplio de varias semanas. Para una entrega de **5 DAGs
> puntuales** se optó por mantener cada pipeline autocontenido en un único
> archivo `.py` (con `dags/common.py` para lo estrictamente compartido): así
> cualquier DAG se entiende leyendo un solo archivo, sin saltar entre módulos,
> lo que facilita tanto el aprendizaje como la revisión/calificación.

## Los 5 DAGs

Los cinco DAGs están pensados para mostrar progresión: del más simple y lineal
(DAG 1) a mecanismos cada vez más específicos de Airflow (XCom intensivo,
branching, TaskGroups, paralelismo con fan-in).

### DAG 1 — `weather_etl_dag`

**Propósito de negocio:** obtener el clima actual de cinco ciudades
latinoamericanas desde la API pública de Open-Meteo (sin API key) y dejarlo
disponible en PostgreSQL para análisis posterior.

**Flujo:**

```text
extraer_datos_clima → transformar_datos_clima → cargar_datos_postgres → generar_resumen
```

| Tarea | Operador | Qué hace |
|---|---|---|
| `extraer_datos_clima` | PythonOperator | Llama a la API REST de Open-Meteo para cada ciudad configurada y guarda el JSON crudo en `data/raw`. |
| `transformar_datos_clima` | PythonOperator | Convierte el JSON en un DataFrame de pandas y agrega una columna derivada (clasificación frío/templado/cálido). Guarda un CSV en `data/processed`. |
| `cargar_datos_postgres` | PythonOperator | Inserta (upsert) el CSV en `taller.clima_observaciones`. |
| `generar_resumen` | PythonOperator | Consulta la tabla recién cargada y registra en el log un resumen agregado por clasificación. |

**Configuración:** dependencia estrictamente lineal, `schedule="@daily"`,
`retries=2` con `retry_delay` de 5 minutos (una API externa puede fallar de
forma transitoria), `catchup=False` para no disparar ejecuciones retroactivas
al desplegar. Es el DAG más simple de los cinco: introduce DAG, task,
operator, dependencia y logging antes de sumar mecanismos más avanzados.

### DAG 2 — `sales_etl_dag`

**Propósito de negocio:** procesar el archivo diario de pedidos de un sistema
de ventas, limpiarlo, calcular métricas por categoría/región y dejar tanto el
detalle como el resumen disponibles en PostgreSQL.

**Flujo:**

```text
generar_datos_ventas_diarias → limpiar_datos_ventas → calcular_metricas_ventas → cargar_ventas_postgres → validar_carga_ventas
```

| Tarea | Operador | Qué hace |
|---|---|---|
| `generar_datos_ventas_diarias` | PythonOperator | Simula la llegada del archivo diario de pedidos (incluye a propósito nulos, duplicados y montos inválidos, como ocurre con datos reales). |
| `limpiar_datos_ventas` | PythonOperator | Con pandas: descarta duplicados, nulos en campos clave y montos no positivos. |
| `calcular_metricas_ventas` | PythonOperator | Agrupa por categoría y región (total vendido, cantidad de pedidos). |
| `cargar_ventas_postgres` | PythonOperator | Carga detalle y resumen en `taller.ventas_detalle` y `taller.ventas_resumen`. |
| `validar_carga_ventas` | PythonOperator | Compara filas cargadas vs. filas realmente presentes en la base (control de calidad de la carga). |

**Configuración:** a diferencia del DAG 1, este hace un uso intensivo de
**XCom**: cada tarea no solo pasa rutas de archivo sino también métricas
(filas originales, filas limpias, filas cargadas) que la siguiente tarea usa
para decidir o validar. `schedule="@daily"`, `catchup=False`.

### DAG 3 — `api_health_monitor_dag`

**Propósito de negocio:** monitorear la disponibilidad de un conjunto de
endpoints HTTP y decidir automáticamente si se debe generar una alerta.

**Flujo:**

```text
                                    ┌─▶ generar_alerta ───────┐
verificar_endpoints → evaluar_estado_general                  ├─▶ generar_reporte_monitoreo
                                    └─▶ registrar_estado_ok ──┘
```

| Tarea | Operador | Qué hace |
|---|---|---|
| `verificar_endpoints` | PythonOperator | Consulta cada endpoint configurado y registra código de estado y latencia. No lanza excepción si un endpoint falla: "el servicio está caído" es un resultado de negocio válido, no un error de ejecución. |
| `evaluar_estado_general` | **BranchPythonOperator** | Decide, según los resultados anteriores, cuál rama seguir. |
| `generar_alerta` | PythonOperator | Rama ejecutada solo si algún endpoint no respondió correctamente. |
| `registrar_estado_ok` | PythonOperator | Rama ejecutada solo si todos los endpoints respondieron correctamente. |
| `generar_reporte_monitoreo` | PythonOperator | Se ejecuta **siempre** (`trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS`) sin importar qué rama corrió, y persiste el resultado en `taller.monitoreo_endpoints`. |

**Configuración:** es el único de los cinco con **lógica condicional**
(branching) y **trigger rules** explícitas. `schedule="*/30 * * * *"`
(cada 30 minutos, cadencia típica de un monitor). Los endpoints se configuran
vía la variable de entorno `TALLER_MONITOR_ENDPOINTS`; por defecto incluye
`httpbin.org/status/500`, que siempre responde con error, para que la rama de
alerta pueda observarse en la demo sin depender de que un servicio real esté
caído en ese momento.

### DAG 4 — `data_quality_ingestion_dag`

**Propósito de negocio:** ingerir un archivo enviado por un socio externo,
validar su calidad y clasificarlo automáticamente como aceptado o rechazado.

**Flujo:**

```text
generar_archivo_entrada → [validar_esquema, validar_valores_nulos, validar_duplicados]  (TaskGroup, en paralelo)
                         → consolidar_resultado_validacion
                         → clasificar_y_mover_archivo
                         → generar_reporte_calidad
```

| Tarea | Operador | Qué hace |
|---|---|---|
| `generar_archivo_entrada` | PythonOperator | Simula la llegada de un archivo con problemas de calidad deliberados (nulos y un duplicado). |
| `validar_esquema` / `validar_valores_nulos` / `validar_duplicados` | PythonOperator (dentro de un **TaskGroup**) | Tres validaciones independientes entre sí; Airflow las ejecuta en paralelo. |
| `consolidar_resultado_validacion` | PythonOperator | Reúne el resultado de las tres validaciones (leídas de XCom con el prefijo del TaskGroup) y decide el veredicto general. |
| `clasificar_y_mover_archivo` | PythonOperator | Mueve el archivo a `data/processed` (aprobado) o `data/quarantine` (rechazado) y registra el resultado en `taller.calidad_archivos`. |
| `generar_reporte_calidad` | PythonOperator | Deja un resumen legible del resultado en el log. |

**Configuración:** único DAG con **TaskGroup** y con `schedule=None`
(disparo manual/por evento): representa un pipeline que en producción se
activaría cuando el archivo efectivamente llega -por ejemplo con un
`FileSensor` o una llamada a la API REST de Airflow-, no en un horario fijo.

### DAG 5 — `backup_cleanup_dag`

**Propósito de negocio:** respaldar las tablas clave del esquema `taller`,
verificar la integridad del backup y aplicar una política de retención.

**Flujo:**

```text
crear_backup_tablas → verificar_integridad_backup ─┐
limpiar_backups_antiguos (en paralelo, sin depender de lo anterior) ─┼─▶ verificar_espacio_almacenamiento → reportar_estado_mantenimiento
```

| Tarea | Operador | Qué hace |
|---|---|---|
| `crear_backup_tablas` | PythonOperator | Exporta las tablas configuradas a CSV y las empaqueta en un `.tar.gz` con timestamp. |
| `verificar_integridad_backup` | PythonOperator | Confirma que el archivo existe, pesa más de 0 bytes y es un `.tar.gz` legible. **Sí** lanza una excepción si falla: un backup corrupto es una falla operativa real, no un resultado de negocio válido (a diferencia de las validaciones del DAG 4). |
| `limpiar_backups_antiguos` | PythonOperator | No depende de las tareas anteriores -corre **en paralelo**- y elimina backups más viejos que `taller_backup_retention_days` (Airflow Variable, 7 días por defecto). |
| `verificar_espacio_almacenamiento` | **BashOperator** | Espera a que terminen integridad *y* limpieza (**fan-in**) y reporta con `du -sh` el espacio usado. |
| `reportar_estado_mantenimiento` | PythonOperator | Consolida todo en `taller.mantenimiento_backups`. |

**Configuración:** único DAG con **ejecución en paralelo con convergencia
(fan-in)**, con **BashOperator** y con una **Airflow Variable**.
`schedule="0 2 * * *"` (2 AM, horario típico de mantenimiento).

---

## Cómo ejecutar el proyecto

Requiere Docker y Docker Compose v2 instalados.

```bash
git clone <url-del-repositorio>
cd Taller-03-ETL

cp .env.example .env
# En Linux, opcionalmente ajustar AIRFLOW_UID en .env con el resultado de `id -u`

docker compose up -d
```

La primera vez, Postgres crea el esquema `taller` automáticamente (vía
`sql/init.sql`) y el servicio `airflow-init` crea la base de datos de
metadatos y el usuario administrador. Verificar el estado de los servicios:

```bash
docker compose ps
```

Cuando `airflow-apiserver` figure como `healthy`, abrir
**http://localhost:8080** e iniciar sesión con `airflow` / `airflow`
(definidos en `.env`).

Para detener el entorno (conservando los datos en el volumen de Postgres):

```bash
docker compose down
```

## Cómo probar cada DAG desde la UI

1. En la vista **DAGs**, confirmar que los cinco aparezcan activos (sin
   pausar) y sin el ícono de error de importación.
2. Entrar a un DAG y revisar la pestaña **Graph**: debe mostrar el flujo de
   tareas descrito en la tabla de cada DAG más arriba.
3. Disparar una ejecución manual con el botón ▶ (**Trigger DAG**).
4. Ir a la pestaña **Grid**: cada tarea debe pasar por `running` (celeste) a
   `success` (verde). En el DAG 3, una de las dos ramas debe verse en gris
   (`skipped`) — es el comportamiento esperado del branching, no un error.
5. Abrir el log de una tarea (clic sobre el cuadro → **Logs**) para ver el
   detalle de lo que hizo (filas cargadas, resultado de una validación, etc.).
6. Repetir el disparo un par de veces para poblar el **historial de
   ejecuciones** (columna izquierda de la vista Grid), que es evidencia de
   que el DAG corre de forma consistente.

## Errores comunes

Problemas concretos detectados al construir y validar este proyecto, y cómo
se resolvieron:

- **`Variable.get() got an unexpected keyword argument 'default_var'`.**
  Airflow 3 introdujo un nuevo Task SDK (`airflow.sdk`); su clase `Variable`
  cambió la firma de `.get()`: ahora el parámetro se llama `default`, no
  `default_var` como en versiones anteriores de Airflow. Aparece en tiempo
  de ejecución de la tarea, no al parsear el DAG, así que conviene probar
  cada tarea que use Variables antes de darla por buena.

- **Un endpoint externo responde `403 Forbidden` sin motivo aparente.**
  Varias APIs (GitHub incluida) rechazan solicitudes sin cabecera
  `User-Agent`. El DAG 3 la incluye explícitamente por esta razón.

- **Un DAG nuevo no aparece en `airflow dags list` ni puede probarse con
  `airflow dags test`.** En Airflow 3 el *DAG Processor* -un componente
  separado del scheduler- es quien parsea los archivos de `dags/` y los
  registra en la base de datos; la UI y la CLI leen desde ahí, no del
  archivo directamente. Con `docker compose up`, el servicio
  `airflow-dag-processor` hace esto de forma continua y automática; al
  probar la CLI de forma manual (fuera de Docker) puede ser necesario
  forzar un parseo con `airflow dags reserialize`.

- **Los archivos generados dentro de Docker quedan con dueño `root` en el
  host (Linux).** Se soluciona definiendo `AIRFLOW_UID` en `.env` con el
  resultado de `id -u`, tal como indica el comentario en `.env.example`.

- **Una tarea parece "colgada" varios minutos después de fallar.** Si falla
  y el DAG tiene configurado `retries`, Airflow espera el `retry_delay`
  configurado (5 minutos en la mayoría de los DAGs de este taller) antes de
  reintentar; no es que el sistema esté congelado. Puede seguirse el
  progreso real en la pestaña **Logs** o en la columna de intentos
  (*try number*) de la vista Grid.

- **`ModuleNotFoundError: No module named 'pandas'` (o `requests`,
  `psycopg2`) al ejecutar un DAG.** La imagen base de Airflow no incluye
  estas librerías. Por eso el proyecto no usa la imagen oficial directamente
  sino que la extiende con un `Dockerfile` propio que instala
  `requirements.txt`.

- **`ImportError` al importar `PostgresHook`.** El *provider* de PostgreSQL
  (`apache-airflow-providers-postgres`) no viene instalado por defecto en la
  imagen base; está incluido explícitamente en `requirements.txt`.
