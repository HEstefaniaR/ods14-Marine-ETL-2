# /opt/airflow/scripts/transform.py
import os
import re
import pandas as pd
import numpy as np


# ==============================================================
# UTILIDADES BASE
# ==============================================================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    EXPLICIT_MAP = {
        "Date (MM-DD-YYYY)": "date",
        "Latitude (degree)": "latitude",
        "Longitude(degree": "longitude",
        "Mesh size (mm)": "mesh_size",
        "Microplastics measurement": "microplastics_measurement",
        "Water Sample Depth (m)": "water_sample_depth",
        "Marine Setting": "marine_setting",
        "Sampling Method": "sampling_method",
        "Concentration class range": "concentration_class_range",
        "Concentration class text": "concentration_class_text",
        "DOI": "doi",
        "OBJECTID": "objectid",
        "decimalLatitude": "decimallatitude",
        "decimalLongitude": "decimallongitude",
        "scientificName": "scientific_name",
    }
    new_cols = {}
    for col in df.columns:
        if col in EXPLICIT_MAP:
            new_cols[col] = EXPLICIT_MAP[col]
            continue
        clean = col.strip().lower()
        clean = re.sub(r"\(.*?\)|\[.*?\]|[^0-9a-z]+", "_", clean)
        clean = re.sub(r"_+", "_", clean).strip("_")
        new_cols[col] = clean
    return df.rename(columns=new_cols)


def read_csv_smart(path: str) -> pd.DataFrame:
    for sep in ["\t", ",", ";"]:
        try:
            df = pd.read_csv(path, sep=sep, low_memory=False, on_bad_lines="skip", encoding="utf-8")
            if df.shape[1] > 1:
                return df
        except Exception:
            continue
    try:
        return pd.read_csv(path, sep=None, engine="python", low_memory=False, on_bad_lines="skip", encoding="utf-8")
    except Exception:
        return pd.DataFrame()


def infer_ocean(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return np.nan
    if lat > 70:
        return "Arctic Ocean"
    return "Pacific Ocean" if lon < -100 else "Atlantic Ocean"


def assign_grid_vectorized(lat_series, lon_series, cell_size=3.0):
    grid_lat = np.floor(lat_series / cell_size) * cell_size + (cell_size / 2)
    grid_lon = np.floor(lon_series / cell_size) * cell_size + (cell_size / 2)
    return grid_lat.round(2), grid_lon.round(2)


# ==============================================================
# LIMPIEZA DE DATOS
# ==============================================================

def clean_microplastics_data(df):
    if df.empty:
        return df
    df = normalize_columns(df)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if {"latitude", "longitude"}.issubset(df.columns):
        df = df[df["latitude"].between(5, 83) & df["longitude"].between(-180, -50)]
    if "ocean" not in df.columns or df["ocean"].isna().all():
        df["ocean"] = df.apply(lambda r: infer_ocean(r.get("latitude"), r.get("longitude")), axis=1)
    return df


def clean_marine_species_data(df):
    if df.empty:
        return df
    df = normalize_columns(df)
    lat_col = next((c for c in ["decimallatitude", "latitude", "lat"] if c in df.columns), None)
    lon_col = next((c for c in ["decimallongitude", "longitude", "lon"] if c in df.columns), None)
    if lat_col and lon_col:
        df = df.rename(columns={lat_col: "decimallatitude", lon_col: "decimallongitude"})
        df["decimallatitude"] = pd.to_numeric(df["decimallatitude"], errors="coerce")
        df["decimallongitude"] = pd.to_numeric(df["decimallongitude"], errors="coerce")
        df = df[df["decimallatitude"].between(5, 83) & df["decimallongitude"].between(-180, -50)]
    if "eventdate" in df.columns:
        df["eventdate"] = pd.to_datetime(df["eventdate"], errors="coerce")
    return df


def clean_climate_data(df):
    if df.empty:
        return df
    df = normalize_columns(df)
    if {"latitude", "longitude"}.issubset(df.columns):
        df = df[df["latitude"].between(5, 83) & df["longitude"].between(-180, -50)]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


# ==============================================================
# PIPELINE PRINCIPAL
# ==============================================================

def transform_data(microplastics_path, species_path, climate_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    df_micro = clean_microplastics_data(read_csv_smart(microplastics_path))
    df_spec = clean_marine_species_data(read_csv_smart(species_path))
    df_clim = clean_climate_data(read_csv_smart(climate_path))

    # Guardar intermedios
    out_micro = os.path.join(output_dir, "microplastics_clean.csv")
    out_species = os.path.join(output_dir, "marine_species_clean.csv")
    out_climate = os.path.join(output_dir, "climate_clean.csv")
    df_micro.to_csv(out_micro, index=False)
    df_spec.to_csv(out_species, index=False)
    df_clim.to_csv(out_climate, index=False)

    CELL_SIZE = 3.0

    # Crear grillas
    if {"latitude", "longitude"}.issubset(df_micro.columns):
        df_micro["grid_lat"], df_micro["grid_lon"] = assign_grid_vectorized(df_micro["latitude"], df_micro["longitude"], CELL_SIZE)
    if {"decimallatitude", "decimallongitude"}.issubset(df_spec.columns):
        df_spec["grid_lat"], df_spec["grid_lon"] = assign_grid_vectorized(df_spec["decimallatitude"], df_spec["decimallongitude"], CELL_SIZE)
    if {"latitude", "longitude"}.issubset(df_clim.columns):
        df_clim["grid_lat"], df_clim["grid_lon"] = assign_grid_vectorized(df_clim["latitude"], df_clim["longitude"], CELL_SIZE)

    # Agregación climática
    if not df_clim.empty:
        df_clim_grid = df_clim.groupby(["grid_lat", "grid_lon", "date"], as_index=False).agg({
            "wave_direction_dominant": "mean",
            "wave_period_max": "mean",
            "wave_height_max": "mean",
            "wind_wave_direction_dominant": "mean",
            "swell_wave_height_max": "mean",
            "ocean": "first",
            "marine_setting": "first"
        })
    else:
        df_clim_grid = pd.DataFrame()

    # Filtrar válidos
    micro_valid = df_micro.dropna(subset=["grid_lat", "grid_lon", "date"]) if not df_micro.empty else pd.DataFrame()
    climate_valid = df_clim_grid.dropna(subset=["grid_lat", "grid_lon", "date"]) if not df_clim_grid.empty else pd.DataFrame()
    species_valid = df_spec.dropna(subset=["grid_lat", "grid_lon"]) if not df_spec.empty else pd.DataFrame()

    # Merge micro + clima
    if not micro_valid.empty and not climate_valid.empty:
        merged_mc = pd.merge_asof(
            micro_valid.sort_values("date"),
            climate_valid.sort_values("date"),
            on="date",
            by=["grid_lat", "grid_lon"],
            direction="nearest",
            tolerance=pd.Timedelta("60D"),
            suffixes=("_micro", "_climate")
        )
    else:
        merged_mc = micro_valid if not micro_valid.empty else df_micro

    # Agregar especies
    if not species_valid.empty:
        species_agg = species_valid.groupby(["grid_lat", "grid_lon"], as_index=False).agg({
            "scientific_name": "first",
            "kingdom": "first",
            "phylum": "first",
            "class": "first",
            "order": "first",
            "family": "first",
            "genus": "first"
        })
        merged_all = pd.merge(merged_mc, species_agg, on=["grid_lat", "grid_lon"], how="left")
    else:
        merged_all = merged_mc

    merged_out = os.path.join(output_dir, "merged_marine_data.csv")
    merged_all.to_csv(merged_out, index=False)

    return {
        "microplastics": out_micro,
        "species": out_species,
        "climate": out_climate,
        "merged": merged_out
    }


# ==============================================================
# BLOQUE DE DEPURACIÓN LOCAL
# ==============================================================

if __name__ == "__main__":
    DATA_DIR = "./data"
    OUTPUT_DIR = "./data/processed_data"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    micro_path = os.path.join(DATA_DIR, "microplastics.csv")
    species_path = os.path.join(DATA_DIR, "marine_species.csv")
    climate_path = os.path.join(DATA_DIR, "marine_climate_history.csv")

    outputs = transform_data(micro_path, species_path, climate_path, OUTPUT_DIR)

    print("\n=== RESULTADOS ===")
    for k, v in outputs.items():
        print(f"{k}: {v}")

    # Mostrar coincidencias
    df_micro = pd.read_csv(outputs["microplastics"], low_memory=False)
    df_species = pd.read_csv(outputs["species"], low_memory=False)
    df_climate = pd.read_csv(outputs["climate"], low_memory=False)
    df_merged = pd.read_csv(outputs["merged"], low_memory=False)

    for df, name in [(df_micro, "micro"), (df_species, "species"), (df_climate, "climate")]:
        if "grid_lat" not in df.columns or "grid_lon" not in df.columns:
            if "latitude" in df.columns and "longitude" in df.columns:
                df["grid_lat"], df["grid_lon"] = assign_grid_vectorized(df["latitude"], df["longitude"])
            elif "decimallatitude" in df.columns and "decimallongitude" in df.columns:
                df["grid_lat"], df["grid_lon"] = assign_grid_vectorized(df["decimallatitude"], df["decimallongitude"])

    coords_micro = set(zip(df_micro["grid_lat"].dropna(), df_micro["grid_lon"].dropna()))
    coords_species = set(zip(df_species["grid_lat"].dropna(), df_species["grid_lon"].dropna()))
    coords_climate = set(zip(df_climate["grid_lat"].dropna(), df_climate["grid_lon"].dropna()))

    inter_micro_climate = coords_micro & coords_climate
    inter_micro_species = coords_micro & coords_species
    inter_all = coords_micro & coords_climate & coords_species

    print("\n=== COINCIDENCIAS DE COORDENADAS ===")
    print(f"Microplastics ↔ Climate: {len(inter_micro_climate)} coincidencias")
    print(f"Microplastics ↔ Species: {len(inter_micro_species)} coincidencias")
    print(f"TODOS (3 conjuntos): {len(inter_all)} coincidencias")
    print(f"Registros finales en merge: {len(df_merged)}")

    if inter_all:
        print("\nEjemplo de coordenadas coincidentes:")
        for coord in list(inter_all)[:5]:
            print(f"  Grid lat/lon: {coord}")
    else:
        print("\nNo hubo coincidencias exactas entre los tres datasets.")