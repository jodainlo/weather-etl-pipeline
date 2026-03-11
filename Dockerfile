FROM apache/airflow:2.8.1

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt