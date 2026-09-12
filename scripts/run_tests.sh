#!/usr/bin/env bash
# Ejecuta la suite de pruebas del proyecto (tests/test_dag_logic.py).
# Requiere el entorno de desarrollo local (ver requirements-dev.txt en la
# raíz del proyecto), no el contenedor Docker.
set -euo pipefail

cd "$(dirname "$0")/.."
echo "Ejecutando pytest sobre tests/ ..."
python3 -m pytest tests/ -v
