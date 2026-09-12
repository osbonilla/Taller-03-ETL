# Imagen base oficial de Apache Airflow. Se fija la version exacta (3.3.1,
# la version estable vigente al construir este taller) y la variante de
# Python 3.12 para reproducibilidad: cualquier persona que construya esta
# imagen obtiene exactamente el mismo entorno.
FROM apache/airflow:3.3.1-python3.12

# Se instalan como usuario "airflow" (no root), siguiendo la practica
# recomendada por la imagen oficial.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
