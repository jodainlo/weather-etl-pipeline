import pandas as pd
import json
import logging
import os
from datetime import datetime
from google.cloud import storage

logger = logging.getLogger(__name__)

def upload_to_gcs(df: pd.DataFrame, bucket_name: str, execution_date: str) -> str:
    """
    Sube los datos transformados a Google Cloud Storage como JSON.
    Organizado en particiones por fecha (data lake pattern).
    
    Args:
        df: DataFrame transformado
        bucket_name: Nombre del bucket GCS
        execution_date: Fecha de ejecución (YYYY-MM-DD)
    Returns:
        GCS URI del archivo subido
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Estructura de data lake particionada por fecha
    year  = execution_date[:4]
    month = execution_date[5:7]
    day   = execution_date[8:10]
    blob_path = f"weather/year={year}/month={month}/day={day}/weather_data.json"
    
    # Convertir a JSON
    records = df.to_dict(orient="records")
    # Serializar fechas correctamente
    for record in records:
        for key, val in record.items():
            if hasattr(val, 'isoformat'):
                record[key] = str(val)
            elif isinstance(val, bool):
                record[key] = val
    
    json_data = json.dumps(records, ensure_ascii=False, indent=2)
    
    # Subir a GCS
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json_data, content_type="application/json")
    
    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    logger.info(f"✓ Datos subidos a GCS: {gcs_uri}")
    return gcs_uri


if __name__ == "__main__":
    from extract import extract_weather_data
    from transform import transform_weather_data
    
    bucket = os.getenv("GCS_BUCKET_NAME", "weather-pipeline-lake-tunombre")
    raw    = extract_weather_data(days_back=7)
    clean  = transform_weather_data(raw)
    uri    = upload_to_gcs(clean, bucket, datetime.today().strftime("%Y-%m-%d"))
    print(f"Archivo en: {uri}")