"""Configuración compartida de pytest.

Agrega la carpeta ``dags/`` al ``sys.path``, replicando lo que Airflow hace
automáticamente con su ``dags_folder`` al parsear DAGs. Así los tests pueden
hacer ``import dag_1_weather_etl`` igual que lo haría Airflow.
"""
import os
import sys

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")
sys.path.insert(0, os.path.abspath(DAGS_DIR))
