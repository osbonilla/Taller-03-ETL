#!/usr/bin/env bash
# Verifica que los 5 DAGs se registren en Airflow sin errores de importación.
# Pensado para ejecutarse DENTRO del entorno de docker-compose, por ejemplo:
#
#   docker compose run --rm airflow-cli bash scripts/verificar_dags.sh
#
set -euo pipefail

echo "Reserializando DAGs..."
airflow dags reserialize

echo ""
echo "=== Errores de importación (deberia decir 'No data found') ==="
airflow dags list-import-errors

echo ""
echo "=== DAGs registrados (deberian aparecer los 5 del taller) ==="
airflow dags list
