# ☁️ Weather ETL Pipeline — Cloud & Orquestación

Pipeline ETL cloud-native completamente automatizado que extrae datos climáticos históricos de 5 ciudades globales desde la API Open-Meteo, los almacena en Google Cloud Storage como data lake, los transforma con métricas enriquecidas y los carga en BigQuery como data warehouse. Todo orquestado con Apache Airflow corriendo en Docker.

---

![Arquitectura del Pipeline](images\weather_architecture.png)

---

## 🏗️ Arquitectura

```
Open-Meteo API (gratuita, sin API key)
         │
         ▼
   [ EXTRACCIÓN ]  ──► Python · requests
         │
         ▼
  [ TRANSFORMACIÓN ] ──► Pandas
   - Limpieza de datos
   - Categoría de temperatura
   - Amplitud térmica
   - Flag de precipitación
   - Descripción del clima (WMO codes)
         │
         ▼
   [ DATA LAKE ] ──► Google Cloud Storage
   - Formato JSON
   - Particionado por fecha (year/month/day)
         │
         ▼
 [ DATA WAREHOUSE ] ──► BigQuery
   - WRITE_APPEND
   - Deduplicación SQL automática
         │
         ▼
  [ ORQUESTACIÓN ] ──► Apache Airflow (Docker)
   - DAG diario a las 6am UTC
   - Retry automático (2 intentos)
   - XCom para pasar datos entre tareas
   - Logging completo
```

---

## 🛠️ Tech Stack

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.8 |
| Extracción | requests · Open-Meteo API |
| Transformación | Pandas · NumPy |
| Data Lake | Google Cloud Storage (GCS) |
| Data Warehouse | BigQuery |
| Orquestación | Apache Airflow 2.8.1 |
| Contenedores | Docker · Docker Compose |
| Autenticación | Google Service Account (JSON) |
| Variables de entorno | python-dotenv |
| Control de versiones | Git · GitHub |

---

## 📁 Estructura del Proyecto

```
weather_pipeline/
│
├── dags/
│   └── weather_etl_dag.py       # DAG de Airflow con 5 tareas
│
├── src/
│   ├── __init__.py
│   ├── extract.py               # Extrae datos de Open-Meteo API
│   ├── transform.py             # Limpia y enriquece los datos
│   ├── load_gcs.py              # Sube JSON particionado a GCS
│   └── load_bigquery.py        # Carga a BigQuery + deduplicación
│
├── config/
│   ├── __init__.py
│   └── gcp_credentials.json    # Credenciales GCP (no se sube a GitHub)
│
├── assets/
│   └── weather_architecture.svg
│
├── logs/                        # Logs de Airflow (auto-generados)
├── plugins/                     # Plugins de Airflow (vacío)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

---

## ⚙️ Instalación y Configuración

### Pre-requisitos
- Docker Desktop instalado y corriendo
- Cuenta en Google Cloud Platform (capa gratuita)
- Git

### 1. Clonar el repositorio
```bash
git clone https://github.com/tu-usuario/weather-etl-pipeline.git
cd weather-etl-pipeline
```

### 2. Configurar Google Cloud

En la consola de GCP ([console.cloud.google.com](https://console.cloud.google.com)):

1. Crear un proyecto nuevo
2. Habilitar las APIs: **Cloud Storage API** y **BigQuery API**
3. Crear una cuenta de servicio con roles **Storage Admin** y **BigQuery Admin**
4. Descargar la clave JSON y colocarla en `config/gcp_credentials.json`
5. Crear un bucket en GCS (región `us-central1`)
6. Crear un dataset en BigQuery llamado `weather_data`

### 3. Configurar variables de entorno

Crear el archivo `.env` en la raíz:
```env
AIRFLOW_UID=50000
GCP_PROJECT_ID=tu-proyecto-id
GCS_BUCKET_NAME=tu-bucket-name
BIGQUERY_DATASET=weather_data
BIGQUERY_TABLE=daily_weather
```

Actualizar también las variables en `docker-compose.yml`:
```yaml
GCP_PROJECT_ID: tu-proyecto-id
GCS_BUCKET_NAME: tu-bucket-name
```

### 4. Construir y levantar los contenedores

```bash
# Inicializar Airflow (solo la primera vez)
docker-compose build
docker-compose up airflow-init

# Levantar todos los servicios
docker-compose up -d
```

### 5. Acceder a la interfaz de Airflow

Abrir en el navegador: **http://localhost:8080**

- Usuario: `admin`
- Contraseña: `admin`

---

## 🚀 Ejecutar el Pipeline

1. En la interfaz de Airflow, activar el toggle del DAG `weather_etl_pipeline`
2. Click en ▶️ **"Trigger DAG"** para ejecutar manualmente
3. Click en el DAG → **"Graph"** para ver el flujo en tiempo real

### Flujo de tareas:
```
extract ──► transform ──► load_gcs ──► load_bigquery ──► deduplicate
  ✅           ✅            ✅             ✅                ✅
```

---

## 📊 Datos en BigQuery

Consulta de ejemplo para verificar los datos:

```sql
SELECT
    city,
    date,
    temp_mean,
    temp_category,
    weather_description,
    precipitation,
    has_precipitation
FROM `tu-proyecto.weather_data.daily_weather`
ORDER BY city, date DESC
LIMIT 20;
```

### Schema de la tabla `daily_weather`

| Columna | Tipo | Descripción |
|---|---|---|
| city | STRING | Nombre de la ciudad |
| date | STRING | Fecha de la medición |
| latitude | FLOAT | Latitud |
| longitude | FLOAT | Longitud |
| temp_max | FLOAT | Temperatura máxima (°C) |
| temp_min | FLOAT | Temperatura mínima (°C) |
| temp_mean | FLOAT | Temperatura media (°C) |
| temp_range | FLOAT | Amplitud térmica (°C) |
| temp_category | STRING | Freezing / Cold / Mild / Warm / Hot |
| precipitation | FLOAT | Precipitación total (mm) |
| has_precipitation | BOOL | Hubo precipitación (True/False) |
| windspeed_max | FLOAT | Velocidad máxima del viento (km/h) |
| weathercode | STRING | Código WMO del clima |
| weather_description | STRING | Descripción legible del clima |
| loaded_at | STRING | Timestamp de carga |

---

## 🗂️ Estructura del Data Lake (GCS)

Los archivos se almacenan particionados por fecha:

```
gs://tu-bucket/
└── weather/
    └── year=2026/
        └── month=03/
            └── day=10/
                └── weather_data.json
```

---

## 🌍 Ciudades Monitoreadas

| Ciudad | País | Latitud | Longitud |
|---|---|---|---|
| New York | 🇺🇸 Estados Unidos | 40.71° N | 74.01° W |
| London | 🇬🇧 Reino Unido | 51.51° N | 0.13° W |
| Tokyo | 🇯🇵 Japón | 35.68° N | 139.65° E |
| Sydney | 🇦🇺 Australia | 33.87° S | 151.21° E |
| Sao Paulo | 🇧🇷 Brasil | 23.55° S | 46.63° W |

---

## ⏰ Automatización

El DAG está configurado para ejecutarse **diariamente a las 6:00 AM UTC**:

```python
schedule_interval="0 6 * * *"
```

Con retry automático de 2 intentos con 5 minutos de espera entre cada uno.

---

## 🔮 Mejoras Futuras

- [ ] Agregar más ciudades y configurarlas dinámicamente
- [ ] Implementar alertas por email cuando el pipeline falla
- [ ] Agregar dashboard en Google Looker Studio
- [ ] Migrar transformaciones a **dbt**
- [ ] Agregar tests unitarios con pytest
- [ ] Implementar monitoreo con Great Expectations

---

## 👤 Autor

**Tu Nombre**
[LinkedIn](https://www.linkedin.com/in/jorge-intriago-data/) · [GitHub](https://github.com/jodainlo/)