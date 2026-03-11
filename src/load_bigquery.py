import pandas as pd
import logging
from google.cloud import bigquery

logger = logging.getLogger(__name__)

def load_to_bigquery(df: pd.DataFrame, project_id: str,
                     dataset_id: str, table_id: str) -> int:
    client    = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Convertir todos los tipos correctamente
    df_bq = df.copy()
    df_bq["date"]                = df_bq["date"].astype(str)
    df_bq["city"]                = df_bq["city"].astype(str)
    df_bq["temp_category"]       = df_bq["temp_category"].astype(str)
    df_bq["weather_description"] = df_bq["weather_description"].astype(str)
    df_bq["loaded_at"]           = df_bq["loaded_at"].astype(str)
    df_bq["weathercode"]         = df_bq["weathercode"].astype(str)
    df_bq["has_precipitation"]   = df_bq["has_precipitation"].astype(bool)
    df_bq["latitude"]            = pd.to_numeric(df_bq["latitude"],     errors="coerce")
    df_bq["longitude"]           = pd.to_numeric(df_bq["longitude"],    errors="coerce")
    df_bq["temp_max"]            = pd.to_numeric(df_bq["temp_max"],     errors="coerce")
    df_bq["temp_min"]            = pd.to_numeric(df_bq["temp_min"],     errors="coerce")
    df_bq["temp_mean"]           = pd.to_numeric(df_bq["temp_mean"],    errors="coerce")
    df_bq["temp_range"]          = pd.to_numeric(df_bq["temp_range"],   errors="coerce")
    df_bq["precipitation"]       = pd.to_numeric(df_bq["precipitation"],errors="coerce")
    df_bq["windspeed_max"]       = pd.to_numeric(df_bq["windspeed_max"],errors="coerce")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(df_bq, table_ref, job_config=job_config)
    job.result()

    rows = len(df_bq)
    logger.info(f"✓ {rows} filas cargadas en BigQuery: {table_ref}")
    return rows


def deduplicate_bigquery(project_id: str, dataset_id: str, table_id: str):
    client    = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
        CREATE OR REPLACE TABLE `{table_ref}` AS
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY city, date
                       ORDER BY loaded_at DESC
                   ) as row_num
            FROM `{table_ref}`
        )
        WHERE row_num = 1
    """
    client.query(query).result()
    logger.info(f"✓ Deduplicación completada en {table_ref}")