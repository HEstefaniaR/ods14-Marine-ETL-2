import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import openmeteo_requests
import requests_cache
from retry_requests import retry
import logging
from datetime import datetime, timedelta
import numpy as np
import os
from typing import Tuple, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_marine_species(csv_path="/opt/airflow/raw_data/marine_species.csv") -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path, sep="\t", quoting=3, on_bad_lines="skip", low_memory=False)
        print(f"'Marine Species' TSV cargado correctamente: {df.shape[0]} filas y {df.shape[1]} columnas")
        return df
    except Exception as e:
        print(f"Error al leer 'Marine Species' TSV: {e}")
        raise

def extract_microplastics(mysql_conn_str="mysql+pymysql://root:root@host.docker.internal:3306/microplastics_db",
                   table_name="microplastics") -> pd.DataFrame:
    try:
        engine = create_engine(mysql_conn_str)
        df = pd.read_sql_table(table_name, con=engine)
        print(f"Tabla 'microplastics' cargada correctamente: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"Error al extraer la tabla 'microplastics' desde MySQL: {e}")
        raise


def extract_marine_climate_data(
    output_csv_path: str = "/opt/airflow/data/marine_climate_history.csv",
    days_per_batch: int = 30,
    earliest_date: str = "1972-01-01"
) -> pd.DataFrame:
    """
    Extrae datos climáticos marinos de forma incremental hacia atrás en el tiempo.
    
    Variables extraídas:
    - wave_direction_dominant: Dirección dominante de olas (grados)
    - wave_period_max: Período máximo de olas (segundos)
    - wave_height_max: Altura máxima de olas (metros)
    - wind_wave_direction_dominant: Dirección dominante del viento (grados)
    - swell_wave_height_max: Altura máxima de oleaje (metros)
    
    Args:
        output_csv_path: Ruta donde se guarda el histórico
        days_per_batch: Días a extraer por ejecución (default: 30)
        earliest_date: Fecha más antigua a extraer
    
    Returns:
        DataFrame con los datos extraídos en esta ejecución
    """
    logger.info("=" * 80)
    logger.info("INICIANDO EXTRACCIÓN INCREMENTAL DE DATOS CLIMÁTICOS MARINOS")
    logger.info("=" * 80)
    
    last_date, history_exists = get_last_extracted_date(output_csv_path)
    
    start_date, end_date = calculate_next_date_range(last_date, days_per_batch, earliest_date)
    
    if start_date is None or end_date is None:
        logger.info("No hay más datos por extraer. Histórico completo.")
        return pd.DataFrame()
    
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)
    
    marine_url = "https://marine-api.open-meteo.com/v1/marine"
    
    all_records = []
    total_locations = len(NORTH_AMERICA_COASTAL_LOCATIONS)
    
    for idx, location in enumerate(NORTH_AMERICA_COASTAL_LOCATIONS):
        try:
            logger.info(f"[{idx + 1}/{total_locations}] Extrayendo: {location['name']}")
            
            params = {
                "latitude": location["lat"],
                "longitude": location["lon"],
                "start_date": start_date,
                "end_date": end_date,
                "daily": [
                    "wave_direction_dominant",      # Dirección dominante de olas
                    "wave_period_max",              # Período máximo de olas
                    "wave_height_max",              # Altura máxima de olas
                    "wind_wave_direction_dominant", # Dirección dominante del viento
                    "swell_wave_height_max"         # Altura máxima de oleaje
                ]
            }
            
            # Llamar a la API
            responses = client.weather_api(marine_url, params=params)
            response = responses[0]
            
            daily = response.Daily()
            
            dates = pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            )
            
            wave_dir = daily.Variables(0).ValuesAsNumpy()
            wave_period = daily.Variables(1).ValuesAsNumpy()
            wave_height = daily.Variables(2).ValuesAsNumpy()
            wind_wave_dir = daily.Variables(3).ValuesAsNumpy()
            swell_height = daily.Variables(4).ValuesAsNumpy()
            
            for i, date in enumerate(dates):
                record = {
                    "date": date.strftime('%Y-%m-%d'),
                    "location_id": location["id"],
                    "location_name": location["name"],
                    "latitude": response.Latitude(),
                    "longitude": response.Longitude(),
                    "wave_direction_dominant": wave_dir[i] if not np.isnan(wave_dir[i]) else None,
                    "wave_period_max": wave_period[i] if not np.isnan(wave_period[i]) else None,
                    "wave_height_max": wave_height[i] if not np.isnan(wave_height[i]) else None,
                    "wind_wave_direction_dominant": wind_wave_dir[i] if not np.isnan(wind_wave_dir[i]) else None,
                    "swell_wave_height_max": swell_height[i] if not np.isnan(swell_height[i]) else None,
                    "extraction_timestamp": datetime.now().isoformat()
                }
                all_records.append(record)
            
            logger.info(f"  ✓ Extraídos {len(dates)} días para {location['name']}")
            
        except Exception as e:
            logger.error(f"  ✗ Error en {location['name']}: {e}")
            continue
    
    if not all_records:
        logger.warning("No se extrajeron datos en esta ejecución.")
        return pd.DataFrame()
    
    df_new = pd.DataFrame(all_records)
    logger.info(f"\n{'='*80}")
    logger.info(f"RESUMEN DE EXTRACCIÓN:")
    logger.info(f"  • Registros nuevos: {len(df_new)}")
    logger.info(f"  • Período: {start_date} a {end_date}")
    logger.info(f"  • Ubicaciones: {total_locations}")
    logger.info(f"{'='*80}\n")
    
    if history_exists and os.path.exists(output_csv_path):
        df_history = pd.read_csv(output_csv_path)
        df_combined = pd.concat([df_history, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['date', 'location_id'], keep='last')
        df_combined = df_combined.sort_values(['date', 'location_id'], ascending=[False, True])
        df_combined.to_csv(output_csv_path, index=False)
        logger.info(f"Histórico actualizado: {len(df_combined)} registros totales")
    else:
        df_new = df_new.sort_values(['date', 'location_id'], ascending=[False, True])
        df_new.to_csv(output_csv_path, index=False)
        logger.info(f"Nuevo histórico creado: {len(df_new)} registros")
    
    return df_new

# Coordenada de costas de norte america
NORTH_AMERICA_COASTAL_LOCATIONS = [
    # COSTA ESTE (Atlántico)
    {"id": "miami_fl", "name": "Miami, Florida", "lat": 25.7617, "lon": -80.1918},
    {"id": "charleston_sc", "name": "Charleston, South Carolina", "lat": 32.7765, "lon": -79.9311},
    {"id": "outer_banks_nc", "name": "Outer Banks, North Carolina", "lat": 35.5585, "lon": -75.4665},
    {"id": "chesapeake_bay_md", "name": "Chesapeake Bay, Maryland", "lat": 38.9784, "lon": -76.4922},
    {"id": "cape_cod_ma", "name": "Cape Cod, Massachusetts", "lat": 41.6688, "lon": -70.2962},
    {"id": "portland_me", "name": "Portland, Maine", "lat": 43.6591, "lon": -70.2568},
    {"id": "halifax_ns", "name": "Halifax, Nova Scotia", "lat": 44.6488, "lon": -63.5752},
    
    # COSTA DEL GOLFO DE MÉXICO
    {"id": "key_west_fl", "name": "Key West, Florida", "lat": 24.5551, "lon": -81.7800},
    {"id": "tampa_bay_fl", "name": "Tampa Bay, Florida", "lat": 27.7676, "lon": -82.6403},
    {"id": "mobile_bay_al", "name": "Mobile Bay, Alabama", "lat": 30.6954, "lon": -88.0399},
    {"id": "galveston_tx", "name": "Galveston, Texas", "lat": 29.3013, "lon": -94.7977},
    {"id": "corpus_christi_tx", "name": "Corpus Christi, Texas", "lat": 27.8006, "lon": -97.3964},
    
    # COSTA OESTE (Pacífico)
    {"id": "san_diego_ca", "name": "San Diego, California", "lat": 32.7157, "lon": -117.1611},
    {"id": "los_angeles_ca", "name": "Los Angeles, California", "lat": 33.7701, "lon": -118.1937},
    {"id": "san_francisco_ca", "name": "San Francisco, California", "lat": 37.7749, "lon": -122.4194},
    {"id": "seattle_wa", "name": "Seattle, Washington", "lat": 47.6062, "lon": -122.3321},
    {"id": "vancouver_bc", "name": "Vancouver, British Columbia", "lat": 49.2827, "lon": -123.1207},
]

# Ultima fecha consultada desde 2023 a 1972
def get_last_extracted_date(history_csv_path: str) -> Tuple[str, bool]:
    """
    Determina la última fecha extraída del histórico.
    
    Returns:
        Tuple con (última_fecha, archivo_existe)
    """
    if os.path.exists(history_csv_path):
        try:
            df_history = pd.read_csv(history_csv_path)
            if len(df_history) > 0 and 'date' in df_history.columns:
                # Encontrar la fecha más reciente ya extraída
                last_date = pd.to_datetime(df_history['date']).max()
                logger.info(f"Última fecha en histórico: {last_date.strftime('%Y-%m-%d')}")
                return last_date.strftime('%Y-%m-%d'), True
        except Exception as e:
            logger.warning(f"Error leyendo histórico: {e}. Se iniciará desde el principio.")
    
    # Si no existe el archivo o está vacío, empezar desde la fecha más reciente disponible
    logger.info("No se encontró histórico. Iniciando desde fecha más reciente.")
    return "2023-12-31", False


def calculate_next_date_range(
    last_extracted_date: str,
    days_per_batch: int = 30,
    earliest_date: str = "1972-01-01"
) -> Tuple[str, str]:
    last_date = pd.to_datetime(last_extracted_date)
    earliest = pd.to_datetime(earliest_date)
    
    end_date = last_date - timedelta(days=1)
    
    start_date = end_date - timedelta(days=days_per_batch - 1)
    
    if start_date < earliest:
        start_date = earliest
    
    if end_date < earliest:
        logger.info("Se alcanzó la fecha más antigua. No hay más datos por extraer.")
        return None, None
    
    logger.info(f"Siguiente rango a extraer: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def get_climate_history_summary(history_csv_path: str = "/opt/airflow/data/marine_climate_history.csv"):
    """
    Muestra un resumen del histórico de datos climáticos.
    """
    if not os.path.exists(history_csv_path):
        logger.info("No existe histórico aún.")
        return
    
    df = pd.read_csv(history_csv_path)
    df['date'] = pd.to_datetime(df['date'])
    
    print("\n" + "="*80)
    print("RESUMEN DEL HISTÓRICO DE DATOS CLIMÁTICOS")
    print("="*80)
    print(f"Total de registros: {len(df):,}")
    print(f"Fecha más reciente: {df['date'].max().strftime('%Y-%m-%d')}")
    print(f"Fecha más antigua: {df['date'].min().strftime('%Y-%m-%d')}")
    print(f"Número de ubicaciones: {df['location_id'].nunique()}")
    print(f"Período cubierto: {(df['date'].max() - df['date'].min()).days} días")
    print("\nUbicaciones registradas:")
    for loc in df['location_name'].unique():
        count = len(df[df['location_name'] == loc])
        print(f"  • {loc}: {count:,} registros")
    print("="*80 + "\n")