# /opt/airflow/scripts/transform.py
import os
import re
import csv
import pandas as pd
import numpy as np

# ============================================================== #
#                      UTILIDADES BASE                           #
# ============================================================== #

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {}
    for col in df.columns:
        clean = col.strip().lower()
        clean = re.sub(r"\(.*?\)", "", clean)
        clean = re.sub(r"\[.*?\]", "", clean)
        clean = re.sub(r"[^0-9a-z]+", "_", clean)
        clean = re.sub(r"_+", "_", clean).strip("_")
        new_cols[col] = clean
    return df.rename(columns=new_cols)


def read_csv_smart(path: str) -> pd.DataFrame:
    """Lee CSV intentando detectar el separador y corrige si todo está en una sola columna."""
    try:
        df = pd.read_csv(path, sep=None, engine="python", low_memory=False)
    except Exception:
        df = pd.read_csv(path, sep=",", low_memory=False)

    # Si vino todo en una columna, forzar coma como separador
    if df.shape[1] == 1:
        try:
            df = pd.read_csv(path, sep=",", low_memory=False)
            print(f"[INFO] Forzado separador ',' en: {path}")
        except Exception:
            print(f"[ERROR] No se pudo leer correctamente el archivo: {path}")
    return df


def find_col(df: pd.DataFrame, candidates, required=True):
    cand_norm = [normalize_columns(pd.DataFrame(columns=[c])).columns[0] for c in candidates]
    norm_map = {normalize_columns(pd.DataFrame(columns=[c])).columns[0]: c for c in df.columns}
    for c in cand_norm:
        if c in norm_map:
            return norm_map[c]
    if required:
        raise KeyError(f"Ninguna de las columnas {candidates} existe en el DataFrame.")
    return None


def infer_ocean(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return np.nan
    if lat > 70:
        return "Arctic Ocean"
    return "Pacific Ocean" if lon < -100 else "Atlantic Ocean"


# ============================================================== #
#                  LIMPIEZA MICROPLÁSTICOS                       #
# ============================================================== #

def clean_microplastics_data(df):
    df = normalize_columns(df)
    df = df[df["latitude"].between(5, 83) & df["longitude"].between(-180, -50)]
    if "date" in df:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ocean"] = df.apply(lambda r: infer_ocean(r["latitude"], r["longitude"]), axis=1)
    return df


# ============================================================== #
#                    LIMPIEZA ESPECIES                           #
# ============================================================== #

def clean_marine_species_data(df):
    df = normalize_columns(df)
    try:
        lat_col = find_col(df, ["decimallatitude", "latitude"])
        lon_col = find_col(df, ["decimallongitude", "longitude"])
    except KeyError:
        print("[ERROR] No se encontraron columnas de coordenadas en species.csv")
        # devuelve DF vacío con MISMAS columnas (para que to_csv escriba encabezados)
        return df.head(0)

    df = df.rename(columns={lat_col: "decimallatitude", lon_col: "decimallongitude"})
    df = df[df["decimallatitude"].between(5, 83) & df["decimallongitude"].between(-180, -50)]
    return df


# ============================================================== #
#                    LIMPIEZA CLIMA                              #
# ============================================================== #

def clean_climate_data(df):
    df = normalize_columns(df)
    df = df[df["latitude"].between(5, 83) & df["longitude"].between(-180, -50)]
    if "date" in df:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


# ============================================================== #
#                    ORQUESTADOR PRINCIPAL                       #
# ============================================================== #

def transform_data(microplastics_path, species_path, climate_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # --- Lectura robusta --- #
    df_micro = read_csv_smart(microplastics_path)
    df_spec  = read_csv_smart(species_path)
    df_clim  = read_csv_smart(climate_path)

    # --- Limpieza --- #
    microplastics_clean  = clean_microplastics_data(df_micro)
    marine_species_clean = clean_marine_species_data(df_spec)
    climate_clean        = clean_climate_data(df_clim)

    #--- Guardado intermedio --- #
    out_micro   = os.path.join(output_dir, "microplastics_clean.csv")
    out_species = os.path.join(output_dir, "marine_species_clean.csv")
    out_climate = os.path.join(output_dir, "climate_clean.csv")

    microplastics_clean.to_csv(out_micro, index=False)
    marine_species_clean.to_csv(out_species, index=False)
    climate_clean.to_csv(out_climate, index=False)

    print(f"[transform] guardado: {out_micro}")
    print(f"[transform] guardado: {out_species}")
    print(f"[transform] guardado: {out_climate}")

        # --- Merge final (asof por fecha dentro del MISMO AÑO + coords, con fallback y por grupos) --- #
    try:
        print("[transform] Iniciando fusión de datasets (asof por fecha dentro del mismo año y celda geo)...")

        def ensure_round_coords(df, lat_col, lon_col, df_name):
            df = df.copy()
            if lat_col in df.columns and lon_col in df.columns:
                df["lat_round"] = pd.to_numeric(df[lat_col], errors="coerce").round(2)
                df["lon_round"] = pd.to_numeric(df[lon_col], errors="coerce").round(2)
            else:
                missing = [c for c in (lat_col, lon_col) if c not in df.columns]
                print(f"[transform] WARN: {df_name} sin columnas {missing}; no se crean lat_round/lon_round.")
            return df

        def ensure_date_year(df, date_col, df_name):
            df = df.copy()
            if date_col in df.columns:
                dc = pd.to_datetime(df[date_col], errors="coerce")
                df["_date_day"] = pd.to_datetime(dc.dt.date)
                df["_year"] = df["_date_day"].dt.year
            else:
                df["_date_day"] = pd.NaT
                df["_year"] = pd.NA
                print(f"[transform] WARN: {df_name} sin columna '{date_col}'")
            return df

        # Preparación
        microplastics_clean  = ensure_round_coords(microplastics_clean,  "latitude",        "longitude",        "microplastics")
        climate_clean        = ensure_round_coords(climate_clean,        "latitude",        "longitude",        "climate")
        marine_species_clean = ensure_round_coords(marine_species_clean, "decimallatitude", "decimallongitude", "species")

        microplastics_clean  = ensure_date_year(microplastics_clean, "date", "microplastics")
        climate_clean        = ensure_date_year(climate_clean,       "date", "climate")

        # FILTRO de claves y tipos
        left = microplastics_clean.dropna(subset=["lat_round","lon_round","_date_day","_year"]).copy()
        right = climate_clean.dropna(subset=["lat_round","lon_round","_date_day","_year"]).copy()

        if len(left) == 0:
            print("[transform] WARN: microplastics vacío tras filtrar; no hay nada que fusionar. Se guardará igual.")
            merged_mc = microplastics_clean.copy()  # conserva todo lo que haya
        elif len(right) == 0:
            print("[transform] WARN: clima vacío tras filtrar; fallback (micro + species).")
            merged_mc = left.copy()
        else:
            # Tipos consistentes
            left["_year"]  = left["_year"].astype("int64")
            right["_year"] = right["_year"].astype("int64")
            for c in ("lat_round","lon_round"):
                left[c]  = left[c].astype(float)
                right[c] = right[c].astype(float)

            # --- ASOF POR GRUPOS para evitar 'left keys must be sorted' global ---
            groups = sorted(set(zip(left["lat_round"], left["lon_round"], left["_year"])))
            out_chunks = []
            tol = pd.Timedelta("90D")

            for (la, lo, yr) in groups:
                l_g = left[(left["lat_round"]==la) & (left["lon_round"]==lo) & (left["_year"]==yr)].copy()
                r_g = right[(right["lat_round"]==la) & (right["lon_round"]==lo) & (right["_year"]==yr)].copy()

                # Si no hay clima en ese grupo, dejamos solo micro de ese grupo
                if len(r_g) == 0:
                    out_chunks.append(l_g)
                    continue

                # Orden estricto SOLO por la clave temporal dentro del grupo
                l_g = l_g.sort_values("_date_day").reset_index(drop=True)
                r_g = r_g.sort_values("_date_day").reset_index(drop=True)

                merged_g = pd.merge_asof(
                    l_g, r_g,
                    left_on="_date_day",
                    right_on="_date_day",
                    direction="nearest",
                    tolerance=tol,
                    suffixes=("_micro","_climate")
                )
                out_chunks.append(merged_g)

            merged_mc = pd.concat(out_chunks, ignore_index=True)
            print(f"[transform] merged_mc shape (micro ⟂ climate asof same-year por grupos): {merged_mc.shape}")

        # Agregar species por coords (si existe)
        if {"lat_round","lon_round"}.issubset(marine_species_clean.columns) and len(marine_species_clean) > 0:
            merged_all = pd.merge(
                merged_mc,
                marine_species_clean,
                how="left",
                on=["lat_round","lon_round"],
                suffixes=("", "_species"),
                copy=False
            )
            print(f"[transform] merged_all (con species) shape: {merged_all.shape}")
        else:
            print("[transform] WARN: species sin coords o vacío; se deja solo micro (+clima si hubo).")
            merged_all = merged_mc

        # Fecha final (si falta, úsala desde _date_day)
        if "date" not in merged_all.columns or merged_all["date"].isna().all():
            merged_all["date"] = merged_all.get("_date_day", pd.NaT)

        # Guardar y copiar a /opt/airflow/data
        merged_out = os.path.join(output_dir, "merged_marine_data.csv")
        merged_all.to_csv(merged_out, index=False)
        print(f"[transform] guardado merge final: {merged_out}")

        try:
            data_dir = "/opt/airflow/data"
            merged_copy = os.path.join(data_dir, "merged_marine_data.csv")
            with open(merged_out, "rb") as src, open(merged_copy, "wb") as dst:
                dst.write(src.read())
            print(f"[transform] copia del merge en: {merged_copy}")
        except Exception as e:
            print(f"[WARN] No se pudo copiar el merge a /opt/airflow/data: {e}")

    except Exception as e:
        print(f"[WARN] No se pudo generar el merge final: {e}")
        merged_out = None



    return {   
        "microplastics": out_micro,
        "species": out_species,
        "climate": out_climate,
        "merged": merged_out
    }

    
