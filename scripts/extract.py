import pandas as pd
from sqlalchemy import create_engine
import openmeteo_requests
import requests_cache
from retry_requests import retry
import logging
from datetime import datetime, timedelta
import numpy as np
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_marine_species(csv_path="/opt/airflow/raw_data/marine_species.csv", sample_fraction=0.5) -> pd.DataFrame:
    """Lee especies GBIF con TAB delimiter - SOLUCIÓN DEFINITIVA."""
    print(f"Leyendo especies desde: {csv_path}")
    
    try:
        df = pd.read_csv(
            csv_path,
            sep="\t",   
            quoting=3,      
            engine="python",   
            encoding="utf-8"
        )
        
        print(f"✓ Especies cargadas: {df.shape[0]} × {df.shape[1]} cols")
        print(f"  Primeras columnas: {list(df.columns[:10])}")
        
        # Verificar columnas GBIF
        expected = ['scientificName', 'kingdom', 'phylum', 'class', 'decimalLatitude', 'decimalLongitude']
        found = [c for c in expected if c in df.columns]
        print(f"  Columnas GBIF encontradas: {len(found)}/{len(expected)}")
        
        # Muestreo
        if sample_fraction < 1.0:
            original_count = len(df)
            df = df.sample(frac=sample_fraction, random_state=42).reset_index(drop=True)
            print(f"  Muestra: {len(df):,} filas ({sample_fraction*100:.0f}% de {original_count:,})")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] No se pudo leer especies: {e}")
        raise

def extract_microplastics(mysql_conn_str="mysql+pymysql://root:root@host.docker.internal:3306/microplastics_db", table_name="microplastics") -> pd.DataFrame:
    try:
        engine = create_engine(mysql_conn_str)
        df = pd.read_sql_table(table_name, con=engine)
        print(f"✓ Microplásticos: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"[ERROR] Microplásticos: {e}")
        raise

def extract_marine_climate_from_grids(
    grids_csv_path: str = "/opt/airflow/processed_data/grids_catalog.csv",
    output_csv_path: str = "/opt/airflow/data/marine_climate_history.csv",
    days_per_batch: int = 365,
    earliest_date: str = "1972-01-01"
) -> pd.DataFrame:
    """Extrae clima desde las grillas de microplásticos (100% coincidencia)."""
    logger.info("=" * 80)
    logger.info("EXTRACCIÓN CLIMÁTICA DESDE GRILLAS DE MICROPLÁSTICOS")
    logger.info("=" * 80)
    
    if not os.path.exists(grids_csv_path):
        logger.warning(f"No existe {grids_csv_path}. Ejecuta transform primero.")
        return pd.DataFrame()
    
    grids = pd.read_csv(grids_csv_path)
    logger.info(f"Grillas cargadas: {len(grids)}")
    
    last_date, history_exists = get_last_extracted_date(output_csv_path)
    start_date, end_date = calculate_next_date_range(last_date, days_per_batch, earliest_date)
    
    if start_date is None:
        logger.info("No hay más datos por extraer.")
        return pd.DataFrame()
    
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)
    
    marine_url = "https://marine-api.open-meteo.com/v1/marine"
    all_records = []
    total_grids = len(grids)
    
    for idx, grid in grids.iterrows():
        grid_id = grid["grid_id"]
        lat = float(grid["grid_lat"])
        lon = float(grid["grid_lon"])
        
        try:
            logger.info(f"[{idx + 1}/{total_grids}] Grid {grid_id} ({lat:.1f}, {lon:.1f})")
            
            params = {
                "latitude": lat,
                "longitude": lon,
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
                    "grid_id": grid_id,
                    "latitude": lat,
                    "longitude": lon,
                    "ocean": "Atlantic Ocean" if lon > -100 else "Pacific Ocean",
                    "marine_setting": "Beach",
                    "wave_direction_dominant": wave_dir[i] if not np.isnan(wave_dir[i]) else None,
                    "wave_period_max": wave_period[i] if not np.isnan(wave_period[i]) else None,
                    "wave_height_max": wave_height[i] if not np.isnan(wave_height[i]) else None,
                    "wind_wave_direction_dominant": wind_wave_dir[i] if not np.isnan(wind_wave_dir[i]) else None,
                    "swell_wave_height_max": swell_height[i] if not np.isnan(swell_height[i]) else None,
                    "extraction_timestamp": datetime.now().isoformat()
                }
                all_records.append(record)
            
            logger.info(f"  ✓ {len(dates)} días")
            
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")
            continue
    
    if not all_records:
        logger.warning("No se extrajeron datos.")
        return pd.DataFrame()
    
    df_new = pd.DataFrame(all_records)
    
    logger.info(f"\nRESUMEN:")
    logger.info(f"  Registros nuevos: {len(df_new):,}")
    logger.info(f"  Grillas cubiertas: {df_new['grid_id'].nunique()}")
    logger.info(f"  Período: {start_date} a {end_date}")
    
    if history_exists and os.path.exists(output_csv_path):
        df_history = pd.read_csv(output_csv_path)
        df_combined = pd.concat([df_history, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['date', 'grid_id'], keep='last')
        df_combined.to_csv(output_csv_path, index=False)
        logger.info(f"Histórico: {len(df_combined):,} registros")
    else:
        df_new.to_csv(output_csv_path, index=False)
        logger.info(f"Nuevo histórico: {len(df_new):,} registros")
    
    return df_new

def get_last_extracted_date(history_csv_path: str):
    if os.path.exists(history_csv_path):
        try:
            df = pd.read_csv(history_csv_path)
            if len(df) > 0 and 'date' in df.columns:
                last_date = pd.to_datetime(df['date']).max()
                logger.info(f"Última fecha: {last_date.strftime('%Y-%m-%d')}")
                return last_date.strftime('%Y-%m-%d'), True
        except:
            pass
    
    logger.info("Iniciando desde fecha más reciente.")
    return "2023-12-31", False

def calculate_next_date_range(last_extracted_date: str, days_per_batch: int, earliest_date: str):
    last_date = pd.to_datetime(last_extracted_date)
    earliest = pd.to_datetime(earliest_date)
    
    end_date = last_date - timedelta(days=1)
    start_date = end_date - timedelta(days=days_per_batch - 1)
    
    if start_date < earliest:
        start_date = earliest
    
    if end_date < earliest:
        logger.info("Fecha más antigua alcanzada.")
        return None, None
    
    logger.info(f"Rango: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def get_climate_history_summary(history_csv_path: str = "/opt/airflow/data/marine_climate_history.csv"):
    if not os.path.exists(history_csv_path):
        logger.info("No existe histórico.")
        return
    
    df = pd.read_csv(history_csv_path)
    df['date'] = pd.to_datetime(df['date'])
    
    print("\n" + "="*80)
    print("RESUMEN HISTÓRICO CLIMÁTICO")
    print("="*80)
    print(f"Total: {len(df):,}")
    print(f"Más reciente: {df['date'].max().strftime('%Y-%m-%d')}")
    print(f"Más antigua: {df['date'].min().strftime('%Y-%m-%d')}")
    if 'grid_id' in df.columns:
        print(f"Grillas: {df['grid_id'].nunique()}")
    print("="*80 + "\n")