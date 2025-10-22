# -*- coding: utf-8 -*-
"""
Carga el merged en un esquema estrella MySQL (marineDB).
Tablas: dim_sampling_method, dim_climate, dim_location, dim_date, dim_species,
        fact_microplastics, microplastics_species_bridge
"""

import os
import math
import pandas as pd
import numpy as np
import mysql.connector
from pathlib import Path

# --------------------------------------------------------------------
# Paths y conexión
# --------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
MERGED_CSV   = PROJECT_ROOT / "processed_data" / "merged_marine_data.csv"

MYSQL = {
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "host": os.getenv("MYSQL_HOST", "host.docker.internal"),  # host MySQL local
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DB", "marineDB"),
    "connection_timeout": 5,
}
DB_NAME = MYSQL["database"]

def _connect_raw(include_db: bool = True):
    cfg = MYSQL.copy()
    if not include_db:
        cfg.pop("database", None)
    return mysql.connector.connect(**cfg)

def connect_db():
    """Crea la BD si no existe y devuelve (conn, cur) conectados a marineDB."""
    print(f"[LOAD] Intentando conectar a {MYSQL['host']}:{MYSQL['port']} DB={DB_NAME}")
    # 1) conecta sin DB y crea si falta
    conn0 = _connect_raw(include_db=False)
    cur0  = conn0.cursor()
    cur0.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cur0.close()
    conn0.close()
    # 2) conecta con DB
    conn = _connect_raw(include_db=True)
    cur  = conn.cursor()
    return conn, cur

# --------------------------------------------------------------------
# DDL (tablas)
# --------------------------------------------------------------------
DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_sampling_method (
        sampling_method_id INT AUTO_INCREMENT PRIMARY KEY,
        sampling_method VARCHAR(200),
        mesh_size_mm FLOAT,
        concentration_class_range VARCHAR(200),
        concentration_class_text VARCHAR(200),
        UNIQUE KEY uq_sampling (sampling_method, mesh_size_mm, concentration_class_range, concentration_class_text)
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
        UNIQUE KEY uq_climate (wave_direction_dominant, wave_period_max, wave_height_max, wind_wave_direction_dominant, swell_wave_height_max)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id INT AUTO_INCREMENT PRIMARY KEY,
        latitude FLOAT,
        longitude FLOAT,
        ocean VARCHAR(100),
        marine_setting VARCHAR(100),
        UNIQUE KEY uq_location (latitude, longitude, ocean, marine_setting)
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
        UNIQUE KEY uq_date (full_date)
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
        taxon_rank VARCHAR(200),
        UNIQUE KEY uq_species (scientific_name, genus, taxon_rank)
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
# Utilidades idempotencia
# --------------------------------------------------------------------
def _normalize_scalar(v):
    """Limpia strings y redondea floats para que el UNIQUE matchee estable."""
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

def get_or_create(cur, table, cols, values):
    """
    Upsert contra el UNIQUE y devuelve el PK:
    INSERT ... ON DUPLICATE KEY UPDATE pk = LAST_INSERT_ID(pk)
    """
    values = normalize_values(values)
    pk = PK_COL[table]
    ph = ", ".join(["%s"] * len(values))
    insert_sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph}) "
        f"ON DUPLICATE KEY UPDATE {pk}=LAST_INSERT_ID({pk})"
    )
    cur.execute(insert_sql, tuple(values))
    return cur.lastrowid

def ensure_schema(cur):
    for sql in DDL:
        cur.execute(sql)

def coalesce(row, *cands):
    for c in cands:
        if c in row and pd.notna(row[c]):
            return row[c]
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
# Carga principal
# --------------------------------------------------------------------
def main():
    if not MERGED_CSV.exists():
        raise FileNotFoundError(f"No existe {MERGED_CSV}")

    print(f"[LOAD*] leyendo {MERGED_CSV}")
    df = pd.read_csv(MERGED_CSV, low_memory=False)
    df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)

    conn, cur = connect_db()
    ensure_schema(cur)

    inserted = 0
    conn.start_transaction()

    for _, row in df.iterrows():
        # ---------- DATE ----------
        date_val = coalesce(row, "date", "date_micro", "date_climate")
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
        lat = coalesce(row, "latitude_micro", "latitude_climate", "latitude")
        lon = coalesce(row, "longitude_micro", "longitude_climate", "longitude")
        ocean = coalesce(row, "ocean_micro", "ocean_climate", "ocean")
        mset  = coalesce(row, "marine_setting_micro", "marine_setting_climate", "marine_setting")
        loc_id = None
        if (lat is not None) and (lon is not None):
            loc_id = get_or_create(
                cur, "dim_location",
                ["latitude", "longitude", "ocean", "marine_setting"],
                [lat, lon, ocean, mset]
            )

        # ---------- CLIMATE ----------
        clim_cols = ["wave_direction_dominant", "wave_period_max", "wave_height_max",
                     "wind_wave_direction_dominant", "swell_wave_height_max"]
        clim_vals = [row.get(c) if c in row else None for c in clim_cols]
        clim_id = None
        if any(pd.notna(v) for v in clim_vals):
            clim_id = get_or_create(cur, "dim_climate", clim_cols, clim_vals)

        # ---------- SAMPLING METHOD ----------
        sm = coalesce(row, "sampling_method_micro", "sampling_method")
        mesh = coalesce(row, "mesh_size_micro", "mesh_size")
        cc_range = coalesce(row, "concentration_class_range_micro", "concentration_class_range")
        cc_text  = coalesce(row, "concentration_class_text_micro", "concentration_class_text")
        sm_id = None
        if sm or mesh or cc_range or cc_text:
            sm_id = get_or_create(
                cur, "dim_sampling_method",
                ["sampling_method", "mesh_size_mm", "concentration_class_range", "concentration_class_text"],
                [sm, mesh, cc_range, cc_text]
            )

        # ---------- FACT ----------
        measurement = coalesce(row, "microplastics_measurement_micro", "microplastics_measurement")
        water_depth = coalesce(row, "water_sample_depth_micro", "water_sample_depth")
        coll_time   = coalesce(row, "collecting_time_min_micro", "collecting_time", "collecting_time_min")
        volunteers  = coalesce(row, "volunteers_number_micro", "volunteers_number", "volunteers_count")
        nurdles     = coalesce(row, "standardized_nurdle_amount_micro", "standardized_nurdle_amount", "standarize_nurdle_amount")
        unit        = coalesce(row, "unit_micro", "unit")

        cur.execute("""
            INSERT INTO fact_microplastics
            (measurement, water_sample_depth, collecting_time_min, volunteers_count, standardize_nurdle_amount, unit,
             dim_date_date_id, dim_climate_climate_id, dim_location_location_id, dim_sampling_method_sampling_method_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (measurement, water_depth, coll_time, volunteers, nurdles, unit,
              date_id, clim_id, loc_id, sm_id))
        fact_id = cur.lastrowid

        # ---------- SPECIES (opcional) ----------
        spec_name = coalesce(row, "scientific_name", "species", "species_clean")
        if spec_name:
            species_vals = {
                "scientific_name": spec_name,
                "kingdom": row.get("kingdom"),
                "phylum": row.get("phylum"),
                "class": row.get("class") if "class" in row else row.get("class_species"),
                "order_name": row.get("order") if "order" in row else row.get("order_species"),
                "family": row.get("family"),
                "genus": row.get("genus"),
                "taxon_rank": row.get("taxon_rank") if "taxon_rank" in row else row.get("taxonrank"),
            }
            species_id = get_or_create(
                cur, "dim_species",
                list(species_vals.keys()),
                list(species_vals.values())
            )
            if fact_id and date_id and species_id:
                cur.execute("""
                    INSERT IGNORE INTO microplastics_species_bridge
                    (fact_microplastics_observation_id, dim_date_date_id, dim_species_species_id)
                    VALUES (%s,%s,%s)
                """, (fact_id, date_id, species_id))

        inserted += 1
        if inserted % 1000 == 0:
            conn.commit()
            print(f"[LOAD*] {inserted} filas procesadas...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"[LOAD*] Carga terminada. Filas fact: {inserted}")

if __name__ == "__main__":
    main()
