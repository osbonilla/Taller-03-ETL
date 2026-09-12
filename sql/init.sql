-- =============================================================================
-- Taller 03 - ETL con Apache Airflow
-- Script de inicialización de la base de datos de negocio.
--
-- Se ejecuta automáticamente la primera vez que se levanta el contenedor de
-- PostgreSQL (ver volumes en docker-compose.yml, carpeta
-- /docker-entrypoint-initdb.d/). Crea un esquema separado ("taller") para los
-- datos que producen los DAGs, distinto del esquema interno que usa Airflow
-- para sus propios metadatos. De esta forma se reutiliza el mismo motor de
-- base de datos sin mezclar ambos tipos de datos.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS taller;

-- -----------------------------------------------------------------------------
-- DAG 1 - weather_etl_dag
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taller.clima_observaciones (
    id                  SERIAL PRIMARY KEY,
    fecha_ejecucion     DATE NOT NULL,
    ciudad              VARCHAR(100) NOT NULL,
    latitud             NUMERIC(9, 4) NOT NULL,
    longitud            NUMERIC(9, 4) NOT NULL,
    temperatura_c       NUMERIC(5, 2) NOT NULL,
    velocidad_viento_kmh NUMERIC(5, 2),
    clasificacion       VARCHAR(20) NOT NULL,
    cargado_en          TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (fecha_ejecucion, ciudad)
);

-- -----------------------------------------------------------------------------
-- DAG 2 - sales_etl_dag
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taller.ventas_detalle (
    id                  SERIAL PRIMARY KEY,
    fecha_ejecucion     DATE NOT NULL,
    id_pedido           VARCHAR(50) NOT NULL,
    categoria           VARCHAR(50) NOT NULL,
    region              VARCHAR(50) NOT NULL,
    monto               NUMERIC(10, 2) NOT NULL,
    cargado_en          TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (fecha_ejecucion, id_pedido)
);

CREATE TABLE IF NOT EXISTS taller.ventas_resumen (
    id                  SERIAL PRIMARY KEY,
    fecha_ejecucion     DATE NOT NULL,
    categoria           VARCHAR(50) NOT NULL,
    region              VARCHAR(50) NOT NULL,
    total_ventas        NUMERIC(12, 2) NOT NULL,
    cantidad_pedidos    INTEGER NOT NULL,
    cargado_en          TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (fecha_ejecucion, categoria, region)
);

-- -----------------------------------------------------------------------------
-- DAG 3 - api_health_monitor_dag
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taller.monitoreo_endpoints (
    id                  SERIAL PRIMARY KEY,
    fecha_hora_chequeo  TIMESTAMP NOT NULL DEFAULT now(),
    endpoint            VARCHAR(255) NOT NULL,
    codigo_estado       INTEGER,
    latencia_ms         NUMERIC(8, 2),
    saludable           BOOLEAN NOT NULL
);

-- -----------------------------------------------------------------------------
-- DAG 4 - data_quality_ingestion_dag
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taller.calidad_archivos (
    id                  SERIAL PRIMARY KEY,
    fecha_hora_proceso  TIMESTAMP NOT NULL DEFAULT now(),
    nombre_archivo      VARCHAR(255) NOT NULL,
    esquema_valido      BOOLEAN NOT NULL,
    nulos_detectados    INTEGER NOT NULL,
    duplicados_detectados INTEGER NOT NULL,
    resultado_final     VARCHAR(20) NOT NULL,
    destino             VARCHAR(255) NOT NULL
);

-- -----------------------------------------------------------------------------
-- DAG 5 - backup_cleanup_dag
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taller.mantenimiento_backups (
    id                  SERIAL PRIMARY KEY,
    fecha_hora          TIMESTAMP NOT NULL DEFAULT now(),
    archivo_backup      VARCHAR(255) NOT NULL,
    tamano_bytes        BIGINT NOT NULL,
    integridad_ok       BOOLEAN NOT NULL,
    backups_eliminados  INTEGER NOT NULL
);
