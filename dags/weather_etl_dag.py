from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, '/opt/airflow')

from src.extract       import extract_weather_data
from src.transform     import transform_weather_data
from src.load_gcs      import upload_to_gcs
from src.load_bigquery import load_to_bigquery, deduplicate_bigquery

# ── Configuración del DAG ──
default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

PROJECT_ID  = os.getenv("GCP_PROJECT_ID",    "weather-pipeline")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME",   "weather-pipeline-lake-tunombre")
DATASET_ID  = os.getenv("BIGQUERY_DATASET",  "weather_data")
TABLE_ID    = os.getenv("BIGQUERY_TABLE",     "daily_weather")


# ── Funciones de cada tarea ──
def task_extract(**context):
    df = extract_weather_data(days_back=7)
    # Pasar datos entre tareas via XCom
    context["ti"].xcom_push(key="raw_records", value=df.to_json())
    return f"{len(df)} registros extraídos"


def task_transform(**context):
    import pandas as pd
    raw_json = context["ti"].xcom_pull(key="raw_records", task_ids="extract")
    df_raw   = pd.read_json(raw_json)
    df_clean = transform_weather_data(df_raw)
    context["ti"].xcom_push(key="clean_records", value=df_clean.to_json())
    return f"{len(df_clean)} registros transformados"


def task_load_gcs(**context):
    import pandas as pd
    clean_json     = context["ti"].xcom_pull(key="clean_records", task_ids="transform")
    df_clean       = pd.read_json(clean_json)
    execution_date = context["ds"]  # fecha de ejecución del DAG (YYYY-MM-DD)
    uri = upload_to_gcs(df_clean, BUCKET_NAME, execution_date)
    return f"Subido a: {uri}"


def task_load_bigquery(**context):
    import pandas as pd
    clean_json = context["ti"].xcom_pull(key="clean_records", task_ids="transform")
    df_clean   = pd.read_json(clean_json)
    rows = load_to_bigquery(df_clean, PROJECT_ID, DATASET_ID, TABLE_ID)
    return f"{rows} filas en BigQuery"


def task_deduplicate(**context):
    deduplicate_bigquery(PROJECT_ID, DATASET_ID, TABLE_ID)
    return "Deduplicación completada"


# ── Definición del DAG ──
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL de clima: Open-Meteo → GCS → BigQuery",
    schedule_interval="0 6 * * *",   # todos los días a las 6am UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "etl", "bigquery", "gcs"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load_gcs = PythonOperator(
        task_id="load_gcs",
        python_callable=task_load_gcs,
    )

    load_bq = PythonOperator(
        task_id="load_bigquery",
        python_callable=task_load_bigquery,
    )

    deduplicate = PythonOperator(
        task_id="deduplicate",
        python_callable=task_deduplicate,
    )

    # Flujo: extract → transform → load_gcs → load_bigquery → deduplicate
    extract >> transform >> load_gcs >> load_bq >> deduplicate