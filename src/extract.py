import requests
import pandas as pd
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Ciudades a monitorear con sus coordenadas
CITIES = [
    {"name": "New York",    "lat": 40.7128,  "lon": -74.0060},
    {"name": "London",      "lat": 51.5074,  "lon": -0.1278},
    {"name": "Tokyo",       "lat": 35.6762,  "lon": 139.6503},
    {"name": "Sydney",      "lat": -33.8688, "lon": 151.2093},
    {"name": "Sao Paulo",   "lat": -23.5505, "lon": -46.6333},
]

def extract_weather_data(days_back: int = 7) -> pd.DataFrame:
    """
    Extrae datos históricos de clima desde Open-Meteo API.
    API gratuita, sin API key requerida.
    
    Args:
        days_back: Días hacia atrás a extraer
    Returns:
        DataFrame con datos crudos de clima
    """
    end_date   = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    all_data = []
    
    for city in CITIES:
        logger.info(f"Extrayendo clima de {city['name']}...")
        
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude":            city["lat"],
            "longitude":           city["lon"],
            "start_date":          start_date,
            "end_date":            end_date,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
                "windspeed_10m_max",
                "weathercode"
            ],
            "timezone": "UTC"
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            daily = data["daily"]
            df = pd.DataFrame({
                "city":            city["name"],
                "latitude":        city["lat"],
                "longitude":       city["lon"],
                "date":            daily["time"],
                "temp_max":        daily["temperature_2m_max"],
                "temp_min":        daily["temperature_2m_min"],
                "temp_mean":       daily["temperature_2m_mean"],
                "precipitation":   daily["precipitation_sum"],
                "windspeed_max":   daily["windspeed_10m_max"],
                "weathercode":     daily["weathercode"],
            })
            
            all_data.append(df)
            logger.info(f"✓ {city['name']}: {len(df)} registros")
            
        except Exception as e:
            logger.error(f"Error extrayendo {city['name']}: {e}")
    
    result = pd.concat(all_data, ignore_index=True)
    logger.info(f"Extracción completada: {len(result)} registros totales")
    return result


if __name__ == "__main__":
    df = extract_weather_data()
    print(df.head(10))
    print(f"\nShape: {df.shape}")