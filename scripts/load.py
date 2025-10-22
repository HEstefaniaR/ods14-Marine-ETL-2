# -*- coding: utf-8 -*-
"""
Carga el merged en un esquema estrella MySQL (marineDB).
OPTIMIZADO: Batch inserts, rutas dinámicas, mejor logging.
"""

import os
import sys
import math
import pandas as pd
import numpy as np
import mysql.connector
from pathlib import Path
from typing import Optional, Dict, List, Tuple

# --------------------------------------------------------------------
# RUTAS DINÁMICAS (compatibles con Airflow y ejecución directa)
# --------------------------------------------------------------------
def get_merged_csv_path() -> Path:
    """
    Busca el archivo merged en múltiples ubicaciones.
    Orden de prioridad:
    1. Argumento de línea de comandos
    2. /opt/airflow/processed_data/
    3. /opt/airflow/data/
    4. ./processed_data/ (relativo)
    """
    if len(sys.argv) > 1:
        custom_path = Path(sys.argv[1])
        if custom_path.exists():
            return custom_path
    
    search_paths = [
        Path("/opt/airflow/processed_data/merged_marine_data.csv"),
        Path("/opt/airflow/data/merged_marine_data.csv"),
        Path(__file__).resolve().parents[1] / "processed_data" / "merged_marine_data.csv",
        Path("./processed_data/merged_marine_data.csv"),
        Path("./merged_marine_data.csv"),
    ]
    
    for path in search_paths:
        if path.exists():
            print(f"[LOAD] ✓ Archivo encontrado: {path}")
            return path
    
    raise FileNotFoundError(
        f"No se encontró merged_marine_data.csv en ninguna ubicación.\n"
        f"Ubicaciones buscadas:\n" + "\n".join(f"  - {p}" for p in search_paths)
    )

MERGED_CSV = get_merged_csv_path()

# --------------------------------------------------------------------
# Configuración MySQL
# --------------------------------------------------------------------
MYSQL = {
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "host": os.getenv("MYSQL_HOST", "host.docker.internal"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DB", "marineDB"),
    "connection_timeout": 10,
    "autocommit": False,
}
DB_NAME = MYSQL["database"]

def _connect_raw(include_db: bool = True):
    cfg = MYSQL.copy()
    if not include_db:
        cfg.pop("database", None)
    return mysql.connector.connect(**cfg)

def connect_db():
    """Crea la BD si no existe y devuelve (conn, cur) conectados a marineDB."""
    print(f"[LOAD] Conectando a {MYSQL['host']}:{MYSQL['port']} DB={DB_NAME}")
    conn0 = _connect_raw(include_db=False)
    cur0  = conn0.cursor()
    cur0.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cur0.close()
    conn0.close()
    
    conn = _connect_raw(include_db=True)
    cur  = conn.cursor()
    return conn, cur

# --------------------------------------------------------------------
# DDL (tablas) - CON ÍNDICES OPTIMIZADOS
# --------------------------------------------------------------------
DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_sampling_method (
        sampling_method_id INT AUTO_INCREMENT PRIMARY KEY,
        sampling_method VARCHAR(200),
        mesh_size_mm FLOAT,
        concentration_class_range VARCHAR(200),
        concentration_class_text VARCHAR(200),
        UNIQUE KEY uq_sampling (sampling_method(100), mesh_size_mm, concentration_class_range(100), concentration_class_text(100))
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_climate (
        climate_id INT AUTO_INCREMENT PRIMARY KEY,
        wave_direction_dominant FLOAT,
        wave_period_max FLOAT,
        wave_height_max FLOAT,
        wind_wave_direction_dominant FLOAT,
        swell_wave_height_max FLOAT,
        UNIQUE KEY uq_climate (wave_direction_dominant, wave_period_max, wave_height_max, 
                               wind_wave_direction_dominant, swell_wave_height_max)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id INT AUTO_INCREMENT PRIMARY KEY,
        latitude FLOAT,
        longitude FLOAT,
        ocean VARCHAR(100),
        marine_setting VARCHAR(100),
        UNIQUE KEY uq_location (latitude, longitude, ocean(50), marine_setting(50))
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INT AUTO_INCREMENT PRIMARY KEY,
        full_date DATE,
        year INT,
        month INT,
        day INT,
        quarter INT,
        decade INT,
        UNIQUE KEY uq_date (full_date),
        INDEX idx_year (year)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_species (
        species_id INT AUTO_INCREMENT PRIMARY KEY,
        scientific_name VARCHAR(300),
        kingdom VARCHAR(200),
        phylum VARCHAR(200),
        class VARCHAR(200),
        order_name VARCHAR(200),
        family VARCHAR(200),
        genus VARCHAR(200),
        UNIQUE KEY uq_species (scientific_name(200), genus(100))
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_microplastics (
        observation_id INT AUTO_INCREMENT PRIMARY KEY,
        measurement FLOAT,
        water_sample_depth FLOAT,
        collecting_time_min FLOAT,
        volunteers_count INT,
        standardize_nurdle_amount FLOAT,
        unit VARCHAR(45),
        dim_date_date_id INT,
        dim_climate_climate_id INT,
        dim_location_location_id INT,
        dim_sampling_method_sampling_method_id INT,
        INDEX idx_date (dim_date_date_id),
        INDEX idx_location (dim_location_location_id),
        FOREIGN KEY (dim_date_date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (dim_climate_climate_id) REFERENCES dim_climate(climate_id),
        FOREIGN KEY (dim_location_location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (dim_sampling_method_sampling_method_id) REFERENCES dim_sampling_method(sampling_method_id)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS microplastics_species_bridge (
        fact_microplastics_observation_id INT,
        dim_date_date_id INT,
        dim_species_species_id INT,
        PRIMARY KEY (fact_microplastics_observation_id, dim_species_species_id),
        FOREIGN KEY (fact_microplastics_observation_id) REFERENCES fact_microplastics(observation_id),
        FOREIGN KEY (dim_date_date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (dim_species_species_id) REFERENCES dim_species(species_id)
    ) ENGINE=InnoDB;
    """
]

PK_COL = {
    "dim_sampling_method": "sampling_method_id",
    "dim_climate": "climate_id",
    "dim_location": "location_id",
    "dim_date": "date_id",
    "dim_species": "species_id",
}

# --------------------------------------------------------------------
# Utilidades idempotencia + CACHE
# --------------------------------------------------------------------
def _normalize_scalar(v):
    """Limpia strings y redondea floats para UNIQUE matchee estable."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, str):
        v2 = v.strip()
        return v2 if v2 != "" else None
    if isinstance(v, (float, np.floating, int, np.integer)):
        return round(float(v), 6)
    return v

def normalize_values(values):
    return [_normalize_scalar(v) for v in values]

# CACHE para evitar lookups repetidos
DIMENSION_CACHE: Dict[str, Dict[tuple, int]] = {
    "dim_date": {},
    "dim_location": {},
    "dim_climate": {},
    "dim_sampling_method": {},
    "dim_species": {},
}

def get_or_create(cur, table, cols, values, use_cache=True):
    """
    Upsert con CACHE para evitar queries repetidos.
    """
    values = normalize_values(values)
    
    # Crear cache key
    cache_key = tuple(values)
    
    # Buscar en cache primero
    if use_cache and cache_key in DIMENSION_CACHE[table]:
        return DIMENSION_CACHE[table][cache_key]
    
    pk = PK_COL[table]
    ph = ", ".join(["%s"] * len(values))
    insert_sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph}) "
        f"ON DUPLICATE KEY UPDATE {pk}=LAST_INSERT_ID({pk})"
    )
    cur.execute(insert_sql, tuple(values))
    pk_value = cur.lastrowid
    
    # Guardar en cache
    if use_cache:
        DIMENSION_CACHE[table][cache_key] = pk_value
    
    return pk_value

def ensure_schema(cur):
    print("[LOAD] Creando esquema de tablas...")
    for sql in DDL:
        cur.execute(sql)
    print("[LOAD] ✓ Esquema creado/verificado")

def coalesce(row, *cands):
    """
    Busca la primera columna disponible (case-insensitive).
    CRÍTICO: Maneja _micro/_climate/_species sufijos.
    """
    col_map = {c.lower(): c for c in row.index}
    
    for c in cands:
        c_lower = c.lower()
        if c_lower in col_map and pd.notna(row[col_map[c_lower]]):
            return row[col_map[c_lower]]
    return None

def build_date_parts(d):
    if pd.isna(d):
        return None
    d = pd.to_datetime(d, errors="coerce")
    if pd.isna(d):
        return None
    return dict(
        full_date=d.date(),
        year=int(d.year),
        month=int(d.month),
        day=int(d.day),
        quarter=int(math.ceil(d.month/3.0)),
        decade=int(d.year//10*10),
    )

# --------------------------------------------------------------------
# BATCH INSERT (para optimizar fact_microplastics)
# --------------------------------------------------------------------
def batch_insert_facts(cur, batch: List[Tuple], batch_size: int = 500):
    """
    Inserta múltiples facts de una vez (más rápido que row-by-row).
    """
    if not batch:
        return []
    
    placeholders = ",".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * len(batch))
    flat_values = [v for row in batch for v in row]
    
    sql = f"""
        INSERT INTO fact_microplastics
        (measurement, water_sample_depth, collecting_time_min, volunteers_count, 
         standardize_nurdle_amount, unit, dim_date_date_id, dim_climate_climate_id, 
         dim_location_location_id, dim_sampling_method_sampling_method_id)
        VALUES {placeholders}
    """
    
    cur.execute(sql, flat_values)
    
    # Obtener los IDs insertados
    last_id = cur.lastrowid
    return list(range(last_id, last_id + len(batch)))

# --------------------------------------------------------------------
# Carga principal OPTIMIZADA
# --------------------------------------------------------------------
def main():
    print("\n" + "="*80)
    print("INICIANDO CARGA A BASE DE DATOS (marineDB)")
    print("="*80)
    
    print(f"[LOAD] Leyendo {MERGED_CSV}")
    df = pd.read_csv(MERGED_CSV, low_memory=False)
    df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)
    
    print(f"[LOAD] Total filas: {len(df):,}")
    print(f"[LOAD] Columnas disponibles: {list(df.columns)[:15]}...")

    conn, cur = connect_db()
    ensure_schema(cur)

    inserted = 0
    skipped = 0
    species_links = 0
    
    # Estadísticas
    stats = {
        "clima_matches": 0,
        "species_matches": 0,
        "no_measurement": 0,
    }
    
    # Batch processing
    BATCH_SIZE = 500
    fact_batch = []
    species_batch = []
    
    conn.start_transaction()

    print("\n[LOAD] Procesando filas...")
    for idx, row in df.iterrows():
        # Progress indicator
        if (idx + 1) % 1000 == 0:
            print(f"[LOAD] Procesadas {idx + 1:,}/{len(df):,} filas ({(idx+1)/len(df)*100:.1f}%)")
        
        # ---------- DATE ----------
        date_val = coalesce(row, "date", "_date_day", "date_micro", "date_climate")
        date_parts = build_date_parts(date_val)
        date_id = None
        if date_parts:
            date_id = get_or_create(
                cur, "dim_date",
                ["full_date", "year", "month", "day", "quarter", "decade"],
                [date_parts["full_date"], date_parts["year"], date_parts["month"],
                 date_parts["day"], date_parts["quarter"], date_parts["decade"]]
            )

        # ---------- LOCATION ----------
        lat = coalesce(row, "latitude", "latitude_micro", "latitude_climate")
        lon = coalesce(row, "longitude", "longitude_micro", "longitude_climate")
        ocean = coalesce(row, "ocean", "ocean_micro", "ocean_climate")
        mset  = coalesce(row, "marine_setting", "marine_setting_micro", "marine_setting_climate")
        loc_id = None
        if (lat is not None) and (lon is not None):
            loc_id = get_or_create(
                cur, "dim_location",
                ["latitude", "longitude", "ocean", "marine_setting"],
                [lat, lon, ocean, mset]
            )

        # ---------- CLIMATE ----------
        wave_dir = coalesce(row, "wave_direction_dominant", "wave_direction_dominant_climate")
        wave_per = coalesce(row, "wave_period_max", "wave_period_max_climate")
        wave_hgt = coalesce(row, "wave_height_max", "wave_height_max_climate")
        wind_dir = coalesce(row, "wind_wave_direction_dominant", "wind_wave_direction_dominant_climate")
        swell_hgt = coalesce(row, "swell_wave_height_max", "swell_wave_height_max_climate")
        
        clim_cols = ["wave_direction_dominant", "wave_period_max", "wave_height_max",
                     "wind_wave_direction_dominant", "swell_wave_height_max"]
        # Convertir NaN a None y asegurar floats
        clim_vals_clean = [None if pd.isna(v) else float(v) for v in [wave_dir, wave_per, wave_hgt, wind_dir, swell_hgt]]

        clim_id = None
        if any(v is not None for v in clim_vals_clean):
            clim_id = get_or_create(cur, "dim_climate", clim_cols, clim_vals_clean)
            stats["clima_matches"] += 1

        # ---------- SAMPLING METHOD ----------
        sm = coalesce(row, "sampling_method", "sampling_method_micro")
        mesh = coalesce(row, "mesh_size", "mesh_size_micro", "mesh_size_mm")
        cc_range = coalesce(row, "concentration_class_range", "concentration_class_range_micro")
        cc_text  = coalesce(row, "concentration_class_text", "concentration_class_text_micro")
        sm_id = None
        if sm or mesh or cc_range or cc_text:
            sm_id = get_or_create(
                cur, "dim_sampling_method",
                ["sampling_method", "mesh_size_mm", "concentration_class_range", "concentration_class_text"],
                [sm, mesh, cc_range, cc_text]
            )

        # ---------- FACT ----------
        measurement = coalesce(row, "microplastics_measurement", "microplastics_measurement_micro")
        water_depth = coalesce(row, "water_sample_depth", "water_sample_depth_micro")
        coll_time   = coalesce(row, "collecting_time_min", "collecting_time", "collecting_time_micro")
        volunteers  = coalesce(row, "volunteers_number", "volunteers_count", "volunteers_number_micro")
        nurdles     = coalesce(row, "standardized_nurdle_amount", "standardized_nurdle_amount_micro", "standardize_nurdle_amount")
        unit        = coalesce(row, "unit", "unit_micro")

        # Validar measurement
        if measurement is None or pd.isna(measurement):
            skipped += 1
            stats["no_measurement"] += 1
            continue

        # Agregar a batch
        fact_batch.append((
            measurement, water_depth, coll_time, volunteers, nurdles, unit,
            date_id, clim_id, loc_id, sm_id
        ))

        # ---------- SPECIES (preparar para batch) ----------
        spec_name = coalesce(row, "scientific_name", "scientificname", "species", "species_clean", "scientific_name_species")
        if spec_name and not pd.isna(spec_name):
            species_vals = {
                "scientific_name": spec_name,
                "kingdom": coalesce(row, "kingdom", "kingdom_species"),
                "phylum": coalesce(row, "phylum", "phylum_species"),
                "class": coalesce(row, "class", "class_species"),
                "order_name": coalesce(row, "order", "order_species", "order_name"),
                "family": coalesce(row, "family", "family_species"),
                "genus": coalesce(row, "genus", "genus_species"),
            }
            species_id = get_or_create(
                cur, "dim_species",
                list(species_vals.keys()),
                list(species_vals.values())
            )
            species_batch.append((None, date_id, species_id))  # fact_id se asignará después
            stats["species_matches"] += 1

        # Procesar batch cuando alcance el tamaño
        if len(fact_batch) >= BATCH_SIZE:
            fact_ids = batch_insert_facts(cur, fact_batch, BATCH_SIZE)
            
            # Insertar species bridges correspondientes
            for i, (_, date_id, species_id) in enumerate(species_batch[-len(fact_batch):]):
                if date_id and species_id:
                    cur.execute("""
                        INSERT IGNORE INTO microplastics_species_bridge
                        (fact_microplastics_observation_id, dim_date_date_id, dim_species_species_id)
                        VALUES (%s,%s,%s)
                    """, (fact_ids[i], date_id, species_id))
                    species_links += 1
            
            inserted += len(fact_batch)
            fact_batch = []
            conn.commit()  # Commit periódico

    # Procesar batch restante
    if fact_batch:
        fact_ids = batch_insert_facts(cur, fact_batch, BATCH_SIZE)
        
        for i, (_, date_id, species_id) in enumerate(species_batch[-len(fact_batch):]):
            if date_id and species_id:
                cur.execute("""
                    INSERT IGNORE INTO microplastics_species_bridge
                    (fact_microplastics_observation_id, dim_date_date_id, dim_species_species_id)
                    VALUES (%s,%s,%s)
                """, (fact_ids[i], date_id, species_id))
                species_links += 1
        
        inserted += len(fact_batch)

    conn.commit()
    
    # Estadísticas finales
    print("\n" + "="*80)
    print("CARGA COMPLETADA")
    print("="*80)
    print(f"✓ Filas insertadas en fact_microplastics: {inserted:,}")
    print(f"✓ Links especies creados: {species_links:,}")
    print(f"✓ Con datos de clima: {stats['clima_matches']:,} ({stats['clima_matches']/max(inserted,1)*100:.1f}%)")
    print(f"✓ Con especies: {stats['species_matches']:,} ({stats['species_matches']/max(inserted,1)*100:.1f}%)")
    print(f"⚠ Filas sin measurement (omitidas): {skipped:,}")
    print("="*80 + "\n")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()