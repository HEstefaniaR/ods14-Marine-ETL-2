import os
import re
import pandas as pd
import numpy as np

# ------------------------------------------------------------
# COLUMNAS FINALES
# ------------------------------------------------------------
MICROPLASTICS_COLUMNS = [
    'date', 'latitude', 'longitude', 'ocean', 'marine_setting',
    'sampling_method', 'concentration_class_range',
    'concentration_class_text', 'unit', 'microplastics_measurement',
    'objectid', 'grid_id', 'grid_lat', 'grid_lon'
]

SPECIES_COLUMNS = [
    'scientific_name', 'kingdom', 'phylum', 'class', 'order', 'family',
    'genus', 'species', 'latitude', 'longitude', 'event_date',
    'year', 'month', 'day', 'depth', 'occurrence_status', 'individual_count',
    'grid_id', 'grid_lat', 'grid_lon'
]

CLIMATE_COLUMNS = [
    'date', 'latitude', 'longitude', 'ocean', 'marine_setting',
    'wave_direction_dominant', 'wave_period_max', 'wave_height_max',
    'wind_wave_direction_dominant', 'swell_wave_height_max',
    'grid_id', 'grid_lat', 'grid_lon'
]

# ------------------------------------------------------------
# MAPEO DIRECTO GBIF -> NUESTRO ESQUEMA
# ------------------------------------------------------------
GBIF_COLUMN_MAP = {
    'scientificName': 'scientific_name',
    'kingdom': 'kingdom',
    'phylum': 'phylum',
    'class': 'class',
    'order': 'order',
    'family': 'family',
    'genus': 'genus',
    'species': 'species',
    'decimalLatitude': 'latitude',
    'decimalLongitude': 'longitude',
    'eventDate': 'event_date',
    'year': 'year',
    'month': 'month',
    'day': 'day',
    'depth': 'depth',
    'occurrenceStatus': 'occurrence_status',
    'individualCount': 'individual_count'
}

def normalize_species_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas GBIF al esquema interno."""
    rename_map = {}
    for gbif_col, our_col in GBIF_COLUMN_MAP.items():
        if gbif_col in df.columns:
            rename_map[gbif_col] = our_col
    
    df = df.rename(columns=rename_map)
    print(f"  Columnas renombradas: {len(rename_map)}")
    return df

def normalize_microplastics_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas de microplásticos."""
    MAP = {
        "date (mm-dd-yyyy)": "date",
        "latitude (degree)": "latitude",
        "longitude(degree)": "longitude",
        "mesh size (mm)": "mesh_size_mm",
        "microplastics measurement": "microplastics_measurement",
        "marine setting": "marine_setting",
        "sampling method": "sampling_method",
        "concentration class range": "concentration_class_range",
        "concentration class text": "concentration_class_text"
    }
    
    rename_map = {}
    for orig_col in df.columns:
        key = orig_col.strip().lower()
        if key in MAP:
            rename_map[orig_col] = MAP[key]
    
    return df.rename(columns=rename_map)

def read_csv_smart(path: str) -> pd.DataFrame:
    """Lee CSV con detección automática."""
    try:
        df = pd.read_csv(path, low_memory=False, encoding='utf-8')
        if df.shape[1] > 1:
            return df
    except:
        pass
    
    for sep in ["\t", ";"]:
        try:
            df = pd.read_csv(path, sep=sep, low_memory=False, encoding='utf-8')
            if df.shape[1] > 1:
                return df
        except:
            continue
    
    return pd.DataFrame()

def infer_ocean(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return None
    return "Arctic Ocean" if lat > 70 else ("Pacific Ocean" if lon < -100 else "Atlantic Ocean")

# ------------------------------------------------------------
# GRILLAS
# ------------------------------------------------------------
def _detect_lat_lon_cols(df: pd.DataFrame):
    """Detecta columnas lat/lon."""
    cols = {c.lower(): c for c in df.columns}
    lat_candidates = [orig for k, orig in cols.items() if "lat" in k]
    lon_candidates = [orig for k, orig in cols.items() if "lon" in k]
    lat_col = lat_candidates[0] if lat_candidates else None
    lon_col = lon_candidates[0] if lon_candidates else None
    return lat_col, lon_col

def create_grid_system(df_base, precision=2.0):
    """Crea sistema de grillas."""
    lat_col, lon_col = _detect_lat_lon_cols(df_base)
    if not lat_col or not lon_col:
        raise KeyError(f"No lat/lon en: {list(df_base.columns)}")

    df_base[lat_col] = pd.to_numeric(df_base[lat_col], errors="coerce")
    df_base[lon_col] = pd.to_numeric(df_base[lon_col], errors="coerce")

    def snap(lat, lon):
        if pd.isna(lat) or pd.isna(lon):
            return (np.nan, np.nan, None)
        grid_lat = round(lat / precision) * precision
        grid_lon = round(lon / precision) * precision
        lat_dir = "N" if grid_lat >= 0 else "S"
        lon_dir = "E" if grid_lon >= 0 else "W"
        grid_id = f"{lat_dir}{abs(grid_lat):05.1f}{lon_dir}{abs(grid_lon):06.1f}".replace(".", "_")
        return grid_lat, grid_lon, grid_id

    snapped = df_base[[lat_col, lon_col]].dropna().apply(
        lambda r: snap(r[lat_col], r[lon_col]), axis=1, result_type="expand"
    )
    
    df_base["grid_lat"] = np.nan
    df_base["grid_lon"] = np.nan
    df_base["grid_id"] = None

    if not snapped.empty:
        idxs = snapped.index
        df_base.loc[idxs, "grid_lat"] = snapped[0].astype(float)
        df_base.loc[idxs, "grid_lon"] = snapped[1].astype(float)
        df_base.loc[idxs, "grid_id"] = snapped[2].astype(str)

    grids = df_base[["grid_id", "grid_lat", "grid_lon"]].drop_duplicates().dropna(subset=["grid_id"])
    print(f"✓ Grillas: {len(grids)} únicas (precision={precision}°)")
    return df_base, grids.reset_index(drop=True)

def assign_to_nearest_grid(df_target, grids_catalog, lat_col=None, lon_col=None, max_dist_km=250):
    """Asigna registros a grillas cercanas."""
    if df_target.empty or grids_catalog.empty:
        return df_target

    if lat_col is None or lon_col is None:
        lat_col, lon_col = _detect_lat_lon_cols(df_target)

    if not lat_col or not lon_col:
        print("  ⚠️ No lat/lon detectadas")
        df_target["grid_id"] = None
        df_target["grid_lat"] = np.nan
        df_target["grid_lon"] = np.nan
        return df_target

    df_target[lat_col] = pd.to_numeric(df_target[lat_col], errors="coerce")
    df_target[lon_col] = pd.to_numeric(df_target[lon_col], errors="coerce")

    grid_coords = grids_catalog[["grid_id", "grid_lat", "grid_lon"]].dropna().values
    df_target["grid_id"] = None
    df_target["grid_lat"] = np.nan
    df_target["grid_lon"] = np.nan

    assigned = 0
    for idx, row in df_target.iterrows():
        lat = row.get(lat_col)
        lon = row.get(lon_col)
        if pd.isna(lat) or pd.isna(lon):
            continue
        dists = np.sqrt((grid_coords[:, 1].astype(float) - lat)**2 + 
                       (grid_coords[:, 2].astype(float) - lon)**2) * 111
        min_idx = np.argmin(dists)
        if dists[min_idx] <= max_dist_km:
            df_target.at[idx, "grid_id"] = grid_coords[min_idx, 0]
            df_target.at[idx, "grid_lat"] = grid_coords[min_idx, 1]
            df_target.at[idx, "grid_lon"] = grid_coords[min_idx, 2]
            assigned += 1

    print(f"✓ Asignados: {assigned}/{len(df_target)} ({assigned/len(df_target)*100:.1f}%)")
    return df_target

# ------------------------------------------------------------
# LIMPIEZA
# ------------------------------------------------------------
def clean_microplastics_data(df):
    if df.empty:
        return df
    
    df = normalize_microplastics_columns(df)
    
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    lat_col, lon_col = _detect_lat_lon_cols(df)
    if lat_col and lon_col:
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
        df = df[df[lat_col].between(5, 83) & df[lon_col].between(-180, -50)]
        df = df.rename(columns={lat_col: "latitude", lon_col: "longitude"})
    
    if "ocean" not in df.columns or df["ocean"].isna().all():
        df["ocean"] = df.apply(lambda r: infer_ocean(r.get("latitude"), r.get("longitude")), axis=1)
    
    print(f"✓ Microplásticos: {len(df)} filas")
    return df

def clean_marine_species_data(df):
    if df.empty:
        return df
    
    print(f"  Limpiando especies (inicial: {len(df)} filas)...")
    
    # CRÍTICO: Renombrar PRIMERO
    df = normalize_species_columns(df)
    
    # Validar scientific_name
    if "scientific_name" not in df.columns:
        print("  ⚠️ ERROR: No se encontró columna scientific_name")
        return pd.DataFrame()
    
    # Filtrar nombres válidos
    df = df[df["scientific_name"].notna()]
    df = df[df["scientific_name"].str.len() >= 3]
    print(f"  Nombres científicos válidos: {len(df)} filas")
    
    # Coordenadas
    lat_col, lon_col = _detect_lat_lon_cols(df)
    if lat_col and lon_col:
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
        df = df[df[lat_col].between(5, 83) & df[lon_col].between(-180, -50)]
        df = df.rename(columns={lat_col: "latitude", lon_col: "longitude"})
    
    # Fechas
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
        if "year" not in df.columns:
            df["year"] = df["event_date"].dt.year
        if "month" not in df.columns:
            df["month"] = df["event_date"].dt.month
        if "day" not in df.columns:
            df["day"] = df["event_date"].dt.day
    
    print(f"✓ Especies: {len(df)} filas finales")
    return df

def clean_climate_data(df):
    if df.empty:
        return df
    
    lat_col, lon_col = _detect_lat_lon_cols(df)
    if lat_col and lon_col:
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
        df = df[df[lat_col].between(5, 83) & df[lon_col].between(-180, -50)]
        df = df.rename(columns={lat_col: "latitude", lon_col: "longitude"})
    
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    print(f"✓ Clima: {len(df)} filas")
    return df

def project_columns_safe(df: pd.DataFrame, cols_to_keep, df_name=""):
    """Proyecta columnas."""
    res = df.reindex(columns=cols_to_keep)
    missing = [c for c in cols_to_keep if c not in df.columns]
    if missing:
        print(f"  [{df_name}] Columnas faltantes: {len(missing)}")
    return res

# ------------------------------------------------------------
# PIPELINE
# ------------------------------------------------------------
def transform_data(microplastics_path, species_path, climate_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    print("\n" + "="*80)
    print("TRANSFORMACIÓN CON GRILLAS (2° precision)")
    print("="*80)

    # 1) Leer
    df_micro = clean_microplastics_data(read_csv_smart(microplastics_path))
    df_spec = clean_marine_species_data(read_csv_smart(species_path))
    df_clim = clean_climate_data(read_csv_smart(climate_path))

    # 2) Grillas
    print("\n--- GRILLAS ---")
    df_micro, grids_catalog = create_grid_system(df_micro, precision=2.0)
    grids_csv = os.path.join(output_dir, "grids_catalog.csv")
    grids_catalog.to_csv(grids_csv, index=False)

    # 3) Asignar
    print("\n--- ASIGNACIÓN ---")
    df_spec = assign_to_nearest_grid(df_spec, grids_catalog)
    df_clim = assign_to_nearest_grid(df_clim, grids_catalog)

    # 4) Proyectar
    df_micro_clean = project_columns_safe(df_micro, MICROPLASTICS_COLUMNS, "Micro")
    df_spec_clean = project_columns_safe(df_spec, SPECIES_COLUMNS, "Species")
    df_clim_clean = project_columns_safe(df_clim, CLIMATE_COLUMNS, "Climate")

    # 5) Guardar
    out_micro = os.path.join(output_dir, "microplastics_clean.csv")
    out_species = os.path.join(output_dir, "marine_species_clean.csv")
    out_climate = os.path.join(output_dir, "climate_clean.csv")
    
    df_micro_clean.to_csv(out_micro, index=False)
    df_spec_clean.to_csv(out_species, index=False)
    df_clim_clean.to_csv(out_climate, index=False)

    # 6) Agregar clima
    print("\n--- AGREGACIÓN ---")
    df_clim_grid = pd.DataFrame()
    if not df_clim_clean.empty and "date" in df_clim_clean.columns:
        tmp = df_clim_clean[df_clim_clean["grid_id"].notna()].copy()
        tmp["month"] = pd.to_datetime(tmp["date"], errors="coerce").dt.month
        df_clim_grid = tmp.groupby(["grid_id", "month"], as_index=False).agg({
            "grid_lat": "first",
            "grid_lon": "first",
            "wave_direction_dominant": "mean",
            "wave_period_max": "mean",
            "wave_height_max": "mean",
            "wind_wave_direction_dominant": "mean",
            "swell_wave_height_max": "mean",
            "ocean": "first"
        })
        print(f"✓ Clima: {len(df_clim_grid)} registros")

    # 7) Agregar especies
    df_spec_grid = pd.DataFrame()
    if not df_spec_clean.empty and "grid_id" in df_spec_clean.columns:
        tmp2 = df_spec_clean[df_spec_clean["grid_id"].notna()].copy()
        df_spec_grid = tmp2.groupby("grid_id", as_index=False).agg({
            "grid_lat": "first",
            "grid_lon": "first",
            "scientific_name": lambda x: "|".join(x.dropna().unique()[:20]),
            "kingdom": lambda x: x.mode()[0] if len(x.mode()) > 0 else None,
            "phylum": lambda x: x.mode()[0] if len(x.mode()) > 0 else None,
            "class": lambda x: x.mode()[0] if len(x.mode()) > 0 else None,
            "order": lambda x: x.mode()[0] if len(x.mode()) > 0 else None,
            "family": lambda x: x.mode()[0] if len(x.mode()) > 0 else None,
            "genus": lambda x: x.mode()[0] if len(x.mode()) > 0 else None
        })
        print(f"✓ Especies: {len(df_spec_grid)} grillas")

    # 8) Merge
    print("\n--- MERGE ---")
    merged = df_micro_clean.copy()
    
    if "date" in merged.columns:
        merged["month"] = pd.to_datetime(merged["date"], errors="coerce").dt.month

    if not df_clim_grid.empty:
        merged = pd.merge(merged, df_clim_grid, on=["grid_id", "month"], how="left", suffixes=("", "_climate"))
        print(f"  Clima unido")

    if not df_spec_grid.empty:
        merged = pd.merge(merged, df_spec_grid, on="grid_id", how="left", suffixes=("", "_species"))
        print(f"  Especies unidas")

    merged_out = os.path.join(output_dir, "merged_marine_data.csv")
    merged.to_csv(merged_out, index=False)
    
    print("\n" + "="*80)
    print(f"✓ COMPLETADO")
    print(f"  Microplásticos: {len(df_micro_clean)}")
    print(f"  Especies: {len(df_spec_clean)}")
    print(f"  Clima: {len(df_clim_clean)}")
    print(f"  Merged: {len(merged)} filas × {len(merged.columns)} cols")
    print("="*80)
    
    return {
        "microplastics": out_micro,
        "species": out_species,
        "climate": out_climate,
        "merged": merged_out,
        "grids": grids_csv,
    }