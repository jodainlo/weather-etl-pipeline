import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# Códigos WMO de clima a descripción legible
WEATHER_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Foggy", 48: "Icy fog", 51: "Light drizzle", 53: "Moderate drizzle",
    55: "Dense drizzle", 61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Heavy thunderstorm"
}

def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y enriquece los datos de clima.
    
    Args:
        df: DataFrame crudo de extract.py
    Returns:
        DataFrame transformado listo para cargar
    """
    logger.info("Iniciando transformación...")
    df_clean = df.copy()
    
    # 1. Tipos de datos
    df_clean["date"]          = pd.to_datetime(df_clean["date"]).dt.date
    df_clean["temp_max"]      = pd.to_numeric(df_clean["temp_max"],    errors="coerce").round(2)
    df_clean["temp_min"]      = pd.to_numeric(df_clean["temp_min"],    errors="coerce").round(2)
    df_clean["temp_mean"]     = pd.to_numeric(df_clean["temp_mean"],   errors="coerce").round(2)
    df_clean["precipitation"] = pd.to_numeric(df_clean["precipitation"], errors="coerce").round(2)
    df_clean["windspeed_max"] = pd.to_numeric(df_clean["windspeed_max"], errors="coerce").round(2)
    df_clean["weathercode"]   = df_clean["weathercode"].fillna(0).astype(int)
    
    # 2. Eliminar duplicados y nulos críticos
    df_clean = df_clean.drop_duplicates(subset=["city", "date"])
    df_clean = df_clean.dropna(subset=["temp_mean", "date"])
    
    # 3. Enriquecer con descripción del clima
    df_clean["weather_description"] = df_clean["weathercode"].map(WEATHER_CODES).fillna("Unknown")
    
    # 4. Clasificar temperatura
    df_clean["temp_category"] = pd.cut(
        df_clean["temp_mean"],
        bins=[-np.inf, 0, 10, 20, 30, np.inf],
        labels=["Freezing", "Cold", "Mild", "Warm", "Hot"]
    ).astype(str)
    
    # 5. Amplitud térmica del día
    df_clean["temp_range"] = (df_clean["temp_max"] - df_clean["temp_min"]).round(2)
    
    # 6. Flag de lluvia
    df_clean["has_precipitation"] = (df_clean["precipitation"] > 0).astype(bool)
    
    # 7. Columna de carga
    df_clean["loaded_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    # 8. Ordenar columnas
    df_clean = df_clean[[
        "city", "date", "latitude", "longitude",
        "temp_max", "temp_min", "temp_mean", "temp_range", "temp_category",
        "precipitation", "has_precipitation",
        "windspeed_max", "weathercode", "weather_description",
        "loaded_at"
    ]]
    
    logger.info(f"Transformación completada: {len(df_clean)} registros")
    return df_clean


if __name__ == "__main__":
    from extract import extract_weather_data
    raw = extract_weather_data(days_back=7)
    clean = transform_weather_data(raw)
    print(clean.head(10))
    print(f"\nColumnas: {clean.columns.tolist()}")