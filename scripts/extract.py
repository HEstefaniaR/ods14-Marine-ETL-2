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

import pandas as pd
import numpy as np

def extract_marine_species(
    csv_path="/opt/airflow/raw_data/marine_species.csv",
    sample_fraction=0.5 
) -> pd.DataFrame:
    """
    Lee el archivo de especies marinas detectando el separador automáticamente.
    Carga solo una fracción del dataset completo (por defecto 50%) para evitar OOM.
    """
    def load_with_params(sep, engine="c"):
        return pd.read_csv(
            csv_path,
            sep=sep,
            engine=engine,
            quoting=3,
            on_bad_lines="skip",
            low_memory=False,
            encoding="utf-8"
        )

    try:
        # Intentar con TAB primero (común en GBIF)
        try:
            df = load_with_params("\t")
            if df.shape[1] > 10:
                print(f"✓ Marine Species cargado con TAB: {df.shape[0]} filas × {df.shape[1]} columnas")
                return df.sample(frac=sample_fraction, random_state=42).reset_index(drop=True)
        except Exception as e:
            print(f"[INFO] TAB falló: {e}, intentando otros separadores...")
        
        # Intentar con detección automática
        try:
            df = load_with_params(None, engine="python")
            if df.shape[1] > 10:
                print(f"✓ Marine Species cargado con auto-detect: {df.shape[0]} filas × {df.shape[1]} columnas")
                return df.sample(frac=sample_fraction, random_state=42).reset_index(drop=True)
        except Exception as e:
            print(f"[INFO] Auto-detect falló: {e}, intentando coma...")
        
        # Fallback: coma
        df = load_with_params(",")
        print(f"✓ Marine Species cargado con coma: {df.shape[0]} filas × {df.shape[1]} columnas")
        
        # Tomar solo una parte
        df = df.sample(frac=sample_fraction, random_state=42).reset_index(drop=True)
        print(f"[INFO] Se tomó una muestra del {sample_fraction*100:.0f}% → {df.shape[0]} filas")
        
        # Validar columnas esperadas
        expected_cols = ["decimalLatitude", "decimalLongitude", "scientificName"]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            print(f"[WARN] Faltan columnas esperadas: {missing}")
            print(f"[INFO] Columnas disponibles: {list(df.columns)[:10]}...")
        
        return df

    except Exception as e:
        print(f"[CRITICAL] Error extrayendo Marine Species: {e}")
        raise
        

def extract_microplastics(mysql_conn_str="mysql+pymysql://root:root@host.docker.internal:3306/microplastics_db",
                   table_name="microplastics") -> pd.DataFrame:
    try:
        engine = create_engine(mysql_conn_str)
        df = pd.read_sql_table(table_name, con=engine)
        print(f"✓ Microplastics cargado: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"[ERROR] al extraer microplastics desde MySQL: {e}")
        raise


def extract_marine_climate_data(
    output_csv_path: str = "/opt/airflow/data/marine_climate_history.csv",
    days_per_batch: int = 30,
    earliest_date: str = "1972-01-01"
) -> pd.DataFrame:
    """Extrae datos climáticos marinos de forma incremental."""
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
                    "wave_direction_dominant",
                    "wave_period_max",
                    "wave_height_max",
                    "wind_wave_direction_dominant",
                    "swell_wave_height_max"
                ]
            }
            
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
                    "latitude": location["lat"],
                    "longitude": location["lon"],
                    "ocean": location["ocean"],
                    "marine_setting": location["marine_setting"],
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

NORTH_AMERICA_COASTAL_LOCATIONS = [
    # Florida / Caribe occidental
    {"id": "florida_southeast", "name": "South Florida Coast", "lat": 25.5, "lon": -79.5, "ocean": "Atlantic Ocean", "marine_setting": "Beach"},
    
    # Atlántico norte (Massachusetts aprox.)
    {"id": "massachusetts_coast", "name": "Cape Cod Area", "lat": 40.5, "lon": -70.5, "ocean": "Atlantic Ocean", "marine_setting": "Beach"},
    
    # Caribe oriental
    {"id": "antigua_barbuda", "name": "Antigua and Barbuda", "lat": 16.5, "lon": -61.5, "ocean": "Atlantic Ocean", "marine_setting": "Coral Reef"},
    {"id": "puerto_rico", "name": "Puerto Rico Region", "lat": 16.5, "lon": -64.5, "ocean": "Atlantic Ocean", "marine_setting": "Coral Reef"},
]


def get_ocean_from_coordinates(lat: float, lon: float, location_name: str = "") -> str:
    if lat > 60:
        return "Arctic Ocean"
    
    if lat < 30 and -98 <= lon <= -80:
        return "Atlantic Ocean"
    
    if lon > -100:
        return "Atlantic Ocean"
    
    if lon < -100:
        return "Pacific Ocean"
    
    return "Atlantic Ocean"


def get_last_extracted_date(history_csv_path: str) -> Tuple[str, bool]:
    if os.path.exists(history_csv_path):
        try:
            df_history = pd.read_csv(history_csv_path)
            if len(df_history) > 0 and 'date' in df_history.columns:
                last_date = pd.to_datetime(df_history['date']).max()
                logger.info(f"Última fecha en histórico: {last_date.strftime('%Y-%m-%d')}")
                return last_date.strftime('%Y-%m-%d'), True
        except Exception as e:
            logger.warning(f"Error leyendo histórico: {e}. Se iniciará desde el principio.")
    
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
    
    if 'ocean' in df.columns:
        print("\nDistribución por Océano:")
        for ocean, count in df['ocean'].value_counts().items():
            print(f"  • {ocean}: {count:,} registros")
    
    print("\nUbicaciones registradas:")
    for loc in df['location_name'].unique():
        count = len(df[df['location_name'] == loc])
        ocean = df[df['location_name'] == loc]['ocean'].iloc[0] if 'ocean' in df.columns else 'N/A'
        print(f"  • {loc} ({ocean}): {count:,} registros")
    print("="*80 + "\n")


# Columnas que se pueden usar en la transformacion
MICROPLASTICS_COLUMNS = [
    'Date (MM-DD-YYYY)',
    'Latitude (degree)',
    'Longitude(degree)',
    'Ocean',
    'Marine Setting',
    'Sampling Method',
    'Mesh size (mm)',
    'Concentration class range',
    'Concentration class text',
    'Unit',
    'Microplastics measurement',
    'Water Sample Depth (m)',
    'Collecting Time (min)',
    'Volunteers Number',
    'Standardized Nurdle Amount'
]

SPECIES_COLUMNS = [
    'scientificName',
    'kingdom',
    'phylum',
    'class',
    'order',
    'family',
    'genus',
    'taxonRank',
    'decimalLatitude',
    'decimalLongitude',
    'eventDate',
    'year',
    'month',
    'day',
    'depth',
    'occurrenceStatus',
    'individualCount'
]

CLIMATE_COLUMNS = [
    'date',
    'location_id',
    'location_name',
    'latitude',
    'longitude',
    'ocean',
    'marine_setting',
    'wave_direction_dominant',
    'wave_period_max',
    'wave_height_max',
    'wind_wave_direction_dominant',
    'swell_wave_height_max'
]