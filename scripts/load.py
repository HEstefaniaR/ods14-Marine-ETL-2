import os
import math
import pandas as pd
import numpy as np
import mysql.connector
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MERGED_CSV = PROJECT_ROOT / "processed_data" / "merged_marine_data.csv"

MYSQL = {
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "host": os.getenv("MYSQL_HOST", "host.docker.internal"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DB", "marineDB"),
    "connection_timeout": 5,
}

def connect_db():
    """Conecta a MySQL y crea BD si no existe."""
    print(f"[LOAD] Conectando a {MYSQL['host']}:{MYSQL['port']}")
    
    cfg = MYSQL.copy()
    cfg.pop("database")
    conn0 = mysql.connector.connect(**cfg)
    cur0 = conn0.cursor()
    cur0.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL['database']}")
    cur0.close()
    conn0.close()
    
    conn = mysql.connector.connect(**MYSQL)
    return conn, conn.cursor()

DDL = [
    """CREATE TABLE IF NOT EXISTS dim_sampling_method (
        sampling_method_id INT AUTO_INCREMENT PRIMARY KEY,
        sampling_method VARCHAR(200),
        concentration_class_range VARCHAR(200),
        concentration_class_text VARCHAR(200),
        UNIQUE KEY uq_sampling (sampling_method, concentration_class_range, concentration_class_text)
    ) ENGINE=InnoDB""",
    
    """CREATE TABLE IF NOT EXISTS dim_climate (
        climate_id INT AUTO_INCREMENT PRIMARY KEY,
        wave_direction_dominant FLOAT,
        wave_period_max FLOAT,
        wave_height_max FLOAT,
        wind_wave_direction_dominant FLOAT,
        swell_wave_height_max FLOAT,
        UNIQUE KEY uq_climate (wave_direction_dominant, wave_period_max, wave_height_max, 
                               wind_wave_direction_dominant, swell_wave_height_max)
    ) ENGINE=InnoDB""",
    
    """CREATE TABLE IF NOT EXISTS dim_location (
        location_id INT AUTO_INCREMENT PRIMARY KEY,
        latitude FLOAT,
        longitude FLOAT,
        ocean VARCHAR(100),
        marine_setting VARCHAR(100),
        UNIQUE KEY uq_location (latitude, longitude, ocean, marine_setting)
    ) ENGINE=InnoDB""",
    
    """CREATE TABLE IF NOT EXISTS dim_date (
        date_id INT AUTO_INCREMENT PRIMARY KEY,
        full_date DATE,
        year INT,
        month INT,
        day INT,
        quarter INT,
        decade INT,
        UNIQUE KEY uq_date (full_date)
    ) ENGINE=InnoDB""",
    
    """CREATE TABLE IF NOT EXISTS dim_species (
        species_id INT AUTO_INCREMENT PRIMARY KEY,
        scientific_name VARCHAR(1000),
        kingdom VARCHAR(200),
        phylum VARCHAR(200),
        class VARCHAR(200),
        order_name VARCHAR(200),
        family VARCHAR(200),
        genus VARCHAR(200),
        UNIQUE KEY uq_species (scientific_name(200), genus(100))
    ) ENGINE=InnoDB""",
    
    """CREATE TABLE IF NOT EXISTS fact_microplastics (
        observation_id INT AUTO_INCREMENT PRIMARY KEY,
        measurement FLOAT,
        unit VARCHAR(45),
        dim_date_date_id INT,
        dim_climate_climate_id INT,
        dim_location_location_id INT,
        dim_sampling_method_sampling_method_id INT,
        FOREIGN KEY (dim_date_date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (dim_climate_climate_id) REFERENCES dim_climate(climate_id),
        FOREIGN KEY (dim_location_location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (dim_sampling_method_sampling_method_id) REFERENCES dim_sampling_method(sampling_method_id)
    ) ENGINE=InnoDB""",
    
    """CREATE TABLE IF NOT EXISTS microplastics_species_bridge (
        fact_microplastics_observation_id INT,
        dim_date_date_id INT,
        dim_species_species_id INT,
        PRIMARY KEY (fact_microplastics_observation_id, dim_species_species_id),
        FOREIGN KEY (fact_microplastics_observation_id) REFERENCES fact_microplastics(observation_id),
        FOREIGN KEY (dim_date_date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (dim_species_species_id) REFERENCES dim_species(species_id)
    ) ENGINE=InnoDB"""
]

PK_MAP = {
    "dim_sampling_method": "sampling_method_id",
    "dim_climate": "climate_id",
    "dim_location": "location_id",
    "dim_date": "date_id",
    "dim_species": "species_id",
}

def norm_val(v):
    """Normaliza valor para inserción."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, str):
        v = v.strip()
        return v if v else None
    if isinstance(v, (float, np.floating, int, np.integer)):
        return round(float(v), 6)
    return v

def get_or_create(cur, table, cols, vals):
    """Upsert y retorna PK."""
    vals = [norm_val(v) for v in vals]
    pk = PK_MAP[table]
    ph = ", ".join(["%s"] * len(vals))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph}) ON DUPLICATE KEY UPDATE {pk}=LAST_INSERT_ID({pk})"
    cur.execute(sql, tuple(vals))
    return cur.lastrowid

def get_val(row, *cols):
    """Obtiene primer valor no-nulo de columnas."""
    for c in cols:
        if c in row.index and pd.notna(row[c]):
            v = row[c]
            if isinstance(v, str) and v.strip():
                return v.strip()
            elif not isinstance(v, str):
                return v
    return None

def main():
    if not MERGED_CSV.exists():
        raise FileNotFoundError(f"No existe {MERGED_CSV}")

    print(f"[LOAD] Leyendo {MERGED_CSV}")
    df = pd.read_csv(MERGED_CSV, low_memory=False)
    print(f"[LOAD] {len(df)} filas, {len(df.columns)} cols")

    conn, cur = connect_db()
    
    # Crear tablas
    for ddl in DDL:
        cur.execute(ddl)
    
    inserted = 0
    skipped = 0
    species_count = 0
    conn.start_transaction()

    for idx, row in df.iterrows():
        # DATE
        date_val = get_val(row, "date")
        date_id = None
        if date_val:
            d = pd.to_datetime(date_val, errors="coerce")
            if pd.notna(d):
                date_id = get_or_create(cur, "dim_date",
                    ["full_date", "year", "month", "day", "quarter", "decade"],
                    [d.date(), int(d.year), int(d.month), int(d.day), 
                     int(math.ceil(d.month/3.0)), int(d.year//10*10)])

        # LOCATION
        lat = get_val(row, "latitude", "latitude_micro")
        lon = get_val(row, "longitude", "longitude_micro")
        loc_id = None
        if lat is not None and lon is not None:
            ocean = get_val(row, "ocean", "ocean_micro")
            mset = get_val(row, "marine_setting", "marine_setting_micro")
            loc_id = get_or_create(cur, "dim_location",
                ["latitude", "longitude", "ocean", "marine_setting"],
                [lat, lon, ocean, mset])

        # CLIMATE
        clim_id = None
        wave_dir = get_val(row, "wave_direction_dominant", "wave_direction_dominant_climate")
        wave_per = get_val(row, "wave_period_max", "wave_period_max_climate")
        wave_hgt = get_val(row, "wave_height_max", "wave_height_max_climate")
        wind_dir = get_val(row, "wind_wave_direction_dominant", "wind_wave_direction_dominant_climate")
        swell = get_val(row, "swell_wave_height_max", "swell_wave_height_max_climate")
        
        if any(v is not None for v in [wave_dir, wave_per, wave_hgt, wind_dir, swell]):
            clim_id = get_or_create(cur, "dim_climate",
                ["wave_direction_dominant", "wave_period_max", "wave_height_max",
                 "wind_wave_direction_dominant", "swell_wave_height_max"],
                [wave_dir, wave_per, wave_hgt, wind_dir, swell])

        # SAMPLING
        sm_id = None
        sm = get_val(row, "sampling_method")
        cc_r = get_val(row, "concentration_class_range")
        cc_t = get_val(row, "concentration_class_text")
        if any(v is not None for v in [sm, cc_r, cc_t]):
            sm_id = get_or_create(cur, "dim_sampling_method",
                ["sampling_method", "concentration_class_range", "concentration_class_text"],
                [sm, cc_r, cc_t])

        # FACT
        meas = get_val(row, "microplastics_measurement")
        unit = get_val(row, "unit")
        
        if meas is None:
            skipped += 1
            continue

        cur.execute("""
            INSERT INTO fact_microplastics
            (measurement, unit, dim_date_date_id, dim_climate_climate_id, 
             dim_location_location_id, dim_sampling_method_sampling_method_id)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (meas, unit, date_id, clim_id, loc_id, sm_id))
        fact_id = cur.lastrowid

        # SPECIES
        sci_name = get_val(row, "scientific_name", "scientific_name_species")
        if sci_name and len(str(sci_name).strip()) >= 2:  # Mínimo 2 caracteres
            kingdom = get_val(row, "kingdom", "kingdom_species")
            phylum = get_val(row, "phylum", "phylum_species")
            cls = get_val(row, "class", "class_species")
            order = get_val(row, "order", "order_species")
            family = get_val(row, "family", "family_species")
            genus = get_val(row, "genus", "genus_species")
            
            species_id = get_or_create(cur, "dim_species",
                ["scientific_name", "kingdom", "phylum", "class", "order_name", "family", "genus"],
                [sci_name, kingdom, phylum, cls, order, family, genus])
            
            if fact_id and date_id and species_id:
                cur.execute("""
                    INSERT IGNORE INTO microplastics_species_bridge
                    (fact_microplastics_observation_id, dim_date_date_id, dim_species_species_id)
                    VALUES (%s,%s,%s)
                """, (fact_id, date_id, species_id))
                species_count += 1

        inserted += 1
        if inserted % 2000 == 0:
            conn.commit()
            print(f"[LOAD] {inserted} procesadas ({species_count} con especies)...")

    conn.commit()
    
    # Stats
    cur.execute("SELECT COUNT(*) FROM fact_microplastics")
    fact_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dim_species")
    species_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM microplastics_species_bridge")
    bridge_total = cur.fetchone()[0]
    
    print("\n" + "="*80)
    print("[LOAD] ✓ COMPLETADO")
    print(f"  Insertadas: {inserted}")
    print(f"  Skipped (sin measurement): {skipped}")
    print(f"  Con especies: {species_count}")
    print(f"\n  TABLAS:")
    print(f"  - fact_microplastics: {fact_total}")
    print(f"  - dim_species: {species_total}")
    print(f"  - bridge: {bridge_total}")
    print("="*80)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()