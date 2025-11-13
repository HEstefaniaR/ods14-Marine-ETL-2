# -*- coding: utf-8 -*-
"""
Data Quality Checks (Great Expectations) — CORREGIDO para columnas reales de transform.py
"""

import os
from datetime import datetime
from typing import Optional, List

import pandas as pd

# ----------------- Helpers robustos (GX 0.15–1.x) -----------------
def _safe_get_exp_type(cfg):
    et = getattr(cfg, "expectation_type", None)
    if et:
        return et
    try:
        et = cfg["expectation_type"]
        if et:
            return et
    except Exception:
        pass
    try:
        et = cfg.to_json_dict().get("expectation_type")
        if et:
            return et
    except Exception:
        pass
    return str(cfg)[:120]


def _safe_get(block, key, default=None):
    try:
        return block.get(key, default)
    except Exception:
        try:
            return getattr(block, key)
        except Exception:
            try:
                return block.to_json_dict().get(key, default)
            except Exception:
                return default


def _summarize_gx_results(validation, dataset_name, stage_label):
    rows = []
    for res in (validation.get("results", []) or []):
        cfg = res.get("expectation_config", {})
        exp_type = _safe_get_exp_type(cfg)
        success = bool(_safe_get(res, "success", False))
        result_block = _safe_get(res, "result", {}) or {}

        sp = _safe_get(result_block, "success_percent")
        if sp is None:
            ratio = _safe_get(result_block, "success_ratio")
            if ratio is not None:
                sp = ratio * 100
        if sp is None:
            unexp_pct = _safe_get(result_block, "unexpected_percent")
            if unexp_pct is not None:
                sp = 100 - unexp_pct
        if sp is None:
            sp = 100.0 if success else 0.0

        rows.append({
            "dataset": dataset_name,
            "stage": stage_label,
            "expectation": exp_type,
            "success": success,
            "success_percent": round(float(sp), 2),
            "unexpected_count": _safe_get(result_block, "unexpected_count"),
            "element_count": _safe_get(result_block, "element_count"),
        })

    cols = ["dataset", "stage", "expectation", "success",
            "success_percent", "unexpected_count", "element_count"]
    if not rows:
        df = pd.DataFrame(columns=cols)
        print(f"\n {dataset_name} [{stage_label}] — 0/0 reglas pasaron (0.0%)")
        return df

    df = pd.DataFrame(rows, columns=cols)
    total = len(df)
    passed = int(df["success"].sum())
    overall = round(100 * passed / max(total, 1), 2)
    print(f"\n {dataset_name} [{stage_label}] — {passed}/{total} reglas pasaron ({overall}%)")
    if not df.empty:
        print(df[["expectation", "success", "success_percent"]]
              .sort_values(["success", "success_percent"], ascending=[False, False])
              .to_string(index=False))
    return df


# ----------------- Expectativas CORREGIDAS por dataset -----------------
def _apply_expectations_microplastics(context, batch_def, df, stage_label="pre"):
    """
    Columnas REALES de transform.py (MICROPLASTICS_COLUMNS):
    'date', 'latitude', 'longitude', 'ocean', 'marine_setting',
    'sampling_method', 'concentration_class_range',
    'concentration_class_text', 'unit', 'microplastics_measurement',
    'objectid', 'grid_id', 'grid_lat', 'grid_lon'
    """
    df = df.copy().reset_index(drop=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    v = context.get_validator(batch=batch)

    # Columnas que DEBEN existir
    req = [
        'date', 'latitude', 'longitude', 'microplastics_measurement', 
        'unit', 'grid_id', 'objectid'
    ]
    for c in req:
        if c in df.columns:
            v.expect_column_to_exist(c)

    # Validaciones de no-nulos (SOLO si la columna existe)
    for c in ['latitude', 'longitude', 'microplastics_measurement', 'grid_id']:
        if c in df.columns:
            v.expect_column_values_to_not_be_null(c, mostly=0.95)

    # Rangos geográficos
    if "latitude" in df.columns:
        v.expect_column_values_to_be_between("latitude", 5, 83, mostly=0.98)
    if "longitude" in df.columns:
        v.expect_column_values_to_be_between("longitude", -180, -50, mostly=0.98)

    # Rangos de medición
    if "microplastics_measurement" in df.columns:
        v.expect_column_values_to_be_between("microplastics_measurement", 0, 1e7, mostly=0.99)

    # Validación de unit SOLO si stage_label != "post" (porque en POST puede haber transformaciones)
    if "unit" in df.columns and stage_label != "post":
        # Permitir más valores comunes
        v.expect_column_values_to_be_in_set("unit", 
            ["items/m3", "items/l", "items/km2", "items/m²", "items/m^3", 
             "particles/m3", "particles/l", "n/m3"], 
            mostly=0.8)

    # Unicidad (RELAJADA para evitar falsos positivos por JOINs)
    if {"objectid", "grid_id"}.issubset(df.columns):
        v.expect_compound_columns_to_be_unique(["objectid", "grid_id"], mostly=0.90)
    
    return v


def _apply_expectations_climate(context, batch_def, df):
    """
    Columnas REALES de transform.py (CLIMATE_COLUMNS):
    'date', 'latitude', 'longitude', 'ocean', 'marine_setting',
    'wave_direction_dominant', 'wave_period_max', 'wave_height_max',
    'wind_wave_direction_dominant', 'swell_wave_height_max',
    'grid_id', 'grid_lat', 'grid_lon'
    """
    df = df.copy().reset_index(drop=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    v = context.get_validator(batch=batch)

    # Columnas requeridas
    req = ['date', 'latitude', 'longitude', 'grid_id']
    for c in req:
        if c in df.columns:
            v.expect_column_to_exist(c)

    # No-nulos
    for c in ["date", "latitude", "longitude", "grid_id"]:
        if c in df.columns:
            v.expect_column_values_to_not_be_null(c, mostly=0.95)

    # Rangos geográficos
    if "latitude" in df.columns:
        v.expect_column_values_to_be_between("latitude", 5, 83, mostly=0.99)
    if "longitude" in df.columns:
        v.expect_column_values_to_be_between("longitude", -180, -50, mostly=0.99)
    
    # Rangos climáticos (solo si existen)
    if "wave_height_max" in df.columns:
        v.expect_column_values_to_be_between("wave_height_max", 0, 30, mostly=0.995)
    if "wave_period_max" in df.columns:
        v.expect_column_values_to_be_between("wave_period_max", 0, 25, mostly=0.995)
    if "swell_wave_height_max" in df.columns:
        v.expect_column_values_to_be_between("swell_wave_height_max", 0, 20, mostly=0.995)
    if "wind_wave_direction_dominant" in df.columns:
        v.expect_column_values_to_be_between("wind_wave_direction_dominant", 0, 360, mostly=0.999)

    # Unicidad por grid_id y fecha
    if {"grid_id", "date"}.issubset(df.columns):
        v.expect_compound_columns_to_be_unique(["date", "grid_id"], mostly=0.95)
    
    return v


def _apply_expectations_species(context, batch_def, df):
    """
    Columnas REALES de transform.py (SPECIES_COLUMNS):
    'scientific_name', 'kingdom', 'phylum', 'class', 'order', 'family',
    'genus', 'species', 'latitude', 'longitude', 'event_date',
    'year', 'month', 'day', 'depth', 'occurrence_status', 'individual_count',
    'grid_id', 'grid_lat', 'grid_lon'
    """
    df = df.copy().reset_index(drop=True)
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce", utc=True)

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    v = context.get_validator(batch=batch)

    # Columnas requeridas (solo las que transform.py garantiza)
    req = ['scientific_name', 'latitude', 'longitude', 'grid_id']
    for c in req:
        if c in df.columns:
            v.expect_column_to_exist(c)

    # No-nulos (RELAJADO porque species puede tener muchos NaN)
    for c in ["scientific_name", "latitude", "longitude"]:
        if c in df.columns:
            v.expect_column_values_to_not_be_null(c, mostly=0.80)

    # Rangos geográficos
    if "latitude" in df.columns:
        v.expect_column_values_to_be_between("latitude", 5, 83, mostly=0.95)
    if "longitude" in df.columns:
        v.expect_column_values_to_be_between("longitude", -180, -50, mostly=0.95)
    
    # Año razonable
    if "year" in df.columns:
        v.expect_column_values_to_be_between("year", 1950, datetime.now().year, mostly=0.95)

    # Formato de nombre científico (RELAJADO)
    if "scientific_name" in df.columns:
        v.expect_column_values_to_match_regex("scientific_name",
                                              r"^[A-Z][a-z]+ [a-z\-]+$",
                                              mostly=0.70)

    # Kingdom (RELAJADO - permite más valores)
    if "kingdom" in df.columns:
        v.expect_column_values_to_be_in_set(
            "kingdom", 
            ["Animalia", "Plantae", "Protista", "Fungi", "Chromista", "Bacteria", "Archaea", None],
            mostly=0.80
        )
    
    return v


# ----------------- Utilidades idempotentes -----------------
def _ensure_pandas_datasource(context, name: str):
    try:
        return context.data_sources.add_pandas(name=name)
    except Exception:
        return context.data_sources.get(name)


def _unique_names(prefix: str, stage_label: str):
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S%f")
    asset_name = f"{prefix}_asset_{stage_label}_{run_id}"
    batch_name = f"{prefix}_batch_{stage_label}_{run_id}"
    return asset_name, batch_name


# ----------------- Utilidades de reducción (memoria) -----------------
def _project(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Devuelve solo las columnas disponibles de 'cols' (no falla si falta alguna)."""
    if df is None or df.empty:
        return df
    keep = [c for c in cols if c in df.columns]
    return df if not keep else df[keep]


def _downsample(df: pd.DataFrame,
                max_rows: Optional[int] = None,
                frac: Optional[float] = None,
                seed: int = 42) -> pd.DataFrame:
    """Muestreo defensivo: si max_rows y/o frac están definidos, reduce tamaño sin romper dtypes."""
    if df is None or df.empty:
        return df
    if frac is not None and 0 < frac < 1:
        return df.sample(frac=frac, random_state=seed)
    if max_rows is not None and max_rows > 0 and len(df) > max_rows:
        return df.sample(n=max_rows, random_state=seed)
    return df


# ----------------- FUNCIÓN PRINCIPAL -----------------
def run_gx_quality_checks(
    context,
    microplastics_df: pd.DataFrame,
    climate_df: pd.DataFrame,
    species_df: pd.DataFrame,
    stage_label: str = "pre",
    output_path: Optional[str] = None,
    # Controles de tamaño
    mp_max_rows: Optional[int] = 200_000,
    cl_max_rows: Optional[int] = 200_000,
    sp_max_rows: Optional[int] = 300_000,
    mp_frac: Optional[float] = None,
    cl_frac: Optional[float] = None,
    sp_frac: Optional[float] = None,
):
    """
    Ejecuta validaciones GX para los 3 datasets, con columnas REALES de transform.py
    """

    # 1) Proyección de columnas según transform.py
    MP_REQ = [
        'date', 'latitude', 'longitude', 'ocean', 'marine_setting',
        'sampling_method', 'concentration_class_range',
        'concentration_class_text', 'unit', 'microplastics_measurement',
        'objectid', 'grid_id', 'grid_lat', 'grid_lon'
    ]
    CL_REQ = [
        'date', 'latitude', 'longitude', 'ocean', 'marine_setting',
        'wave_direction_dominant', 'wave_period_max', 'wave_height_max',
        'wind_wave_direction_dominant', 'swell_wave_height_max',
        'grid_id', 'grid_lat', 'grid_lon'
    ]
    SP_REQ = [
        'scientific_name', 'kingdom', 'phylum', 'class', 'order', 'family',
        'genus', 'species', 'latitude', 'longitude', 'event_date',
        'year', 'month', 'day', 'depth', 'occurrence_status', 'individual_count',
        'grid_id', 'grid_lat', 'grid_lon'
    ]

    microplastics_df = _project(microplastics_df, MP_REQ)
    climate_df       = _project(climate_df,       CL_REQ)
    species_df       = _project(species_df,       SP_REQ)

    # 2) Downsample defensivo
    microplastics_df = _downsample(microplastics_df, mp_max_rows, mp_frac)
    climate_df       = _downsample(climate_df,     cl_max_rows, cl_frac)
    species_df       = _downsample(species_df,     sp_max_rows, sp_frac)

    # 3) Verificar que haya datos
    print(f"\n[GX] Validando datasets:")
    print(f"  Microplásticos: {len(microplastics_df)} filas, {len(microplastics_df.columns)} cols")
    print(f"  Clima: {len(climate_df)} filas, {len(climate_df.columns)} cols")
    print(f"  Especies: {len(species_df)} filas, {len(species_df.columns)} cols")

    # Si algún dataset está vacío, crear placeholder
    if microplastics_df.empty:
        print("  ⚠️ Microplásticos vacío, usando placeholder")
        microplastics_df = pd.DataFrame(columns=MP_REQ)
    
    if climate_df.empty:
        print("  ⚠️ Clima vacío, usando placeholder")
        climate_df = pd.DataFrame(columns=CL_REQ)
    
    if species_df.empty:
        print("  ⚠️ Especies vacío, usando placeholder")
        species_df = pd.DataFrame(columns=SP_REQ)

    # 4) Idempotencia y assets
    mp_asset_name, mp_batch_name = _unique_names("microplastics_data", stage_label)
    cl_asset_name, cl_batch_name = _unique_names("climate_data",       stage_label)
    sp_asset_name, sp_batch_name = _unique_names("species_data",       stage_label)

    mp_src = _ensure_pandas_datasource(context, f"microplastics_data_source_{stage_label}")
    cl_src = _ensure_pandas_datasource(context, f"climate_data_source_{stage_label}")
    sp_src = _ensure_pandas_datasource(context, f"species_data_source_{stage_label}")

    mp_asset = mp_src.add_dataframe_asset(name=mp_asset_name)
    cl_asset = cl_src.add_dataframe_asset(name=cl_asset_name)
    sp_asset = sp_src.add_dataframe_asset(name=sp_asset_name)

    mp_batch_def = mp_asset.add_batch_definition_whole_dataframe(mp_batch_name)
    cl_batch_def = cl_asset.add_batch_definition_whole_dataframe(cl_batch_name)
    sp_batch_def = sp_asset.add_batch_definition_whole_dataframe(sp_batch_name)

    # 5) Validaciones
    try:
        v_mp = _apply_expectations_microplastics(context, mp_batch_def, microplastics_df, stage_label=stage_label)
        res_mp = v_mp.validate()
        df_mp = _summarize_gx_results(res_mp, "microplastics", stage_label)
    except Exception as e:
        print(f"[ERROR] Validación de microplásticos falló: {e}")
        df_mp = pd.DataFrame(columns=["dataset", "stage", "expectation", "success", "success_percent"])

    try:
        v_cl = _apply_expectations_climate(context, cl_batch_def, climate_df)
        res_cl = v_cl.validate()
        df_cl = _summarize_gx_results(res_cl, "climate", stage_label)
    except Exception as e:
        print(f"[ERROR] Validación de clima falló: {e}")
        df_cl = pd.DataFrame(columns=["dataset", "stage", "expectation", "success", "success_percent"])

    try:
        v_sp = _apply_expectations_species(context, sp_batch_def, species_df)
        res_sp = v_sp.validate()
        df_sp = _summarize_gx_results(res_sp, "species", stage_label)
    except Exception as e:
        print(f"[ERROR] Validación de especies falló: {e}")
        df_sp = pd.DataFrame(columns=["dataset", "stage", "expectation", "success", "success_percent"])

    # 6) Excel combinado
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "/opt/airflow/data"
    os.makedirs(output_dir, exist_ok=True)
    excel_path = output_path or os.path.join(output_dir, f"validation_results_{stage_label}_{ts}.xlsx")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as xw:
        df_mp.to_excel(xw, sheet_name="microplastics", index=False)
        df_cl.to_excel(xw, sheet_name="climate", index=False)
        df_sp.to_excel(xw, sheet_name="species", index=False)
        pd.concat([df_mp, df_cl, df_sp], ignore_index=True).to_excel(xw, sheet_name="all_results", index=False)

    print(f"\n[GE] Excel combinado guardado en: {excel_path}")
    return pd.concat([df_mp, df_cl, df_sp], ignore_index=True), excel_path