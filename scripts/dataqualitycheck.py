# -*- coding: utf-8 -*-
"""
Data Quality Checks (Great Expectations) — versión idempotente y optimizada
- Nombres únicos por corrida (assets & batches)
- Reutiliza datasources si ya existen
- En POST se omite la regla de dominio de 'unit'
- Optimiza memoria: proyección de columnas mínimas y muestreo (downsampling)
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

    # proteger caso sin resultados
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


# ----------------- Expectativas por dataset (alineadas) -----------------
def _apply_expectations_microplastics(context, batch_def, df, stage_label="pre"):
    """
    Columnas objetivo:
    objectid, date, latitude, longitude, region, country, ocean, marine_setting,
    microplastics_measurement, unit, mesh_size, doi

    Nota: en 'post' se omite la regla de dominio de 'unit' (in_set),
    manteniendo sólo no-nulos y el resto de validaciones.
    """
    df = df.copy().reset_index(drop=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    v = context.get_validator(batch=batch)

    req = [
        "objectid", "date", "latitude", "longitude", "region", "country", "ocean",
        "marine_setting", "microplastics_measurement", "unit", "mesh_size", "doi"
    ]
    for c in req:
        if c in df.columns:
            v.expect_column_to_exist(c)

    for c in ["objectid", "date", "latitude", "longitude", "microplastics_measurement", "unit"]:
        if c in df.columns:
            v.expect_column_values_to_not_be_null(c)

    if "latitude" in df.columns:
        v.expect_column_values_to_be_between("latitude", -90, 90)
    if "longitude" in df.columns:
        v.expect_column_values_to_be_between("longitude", -180, 180)
    if "mesh_size" in df.columns:
        v.expect_column_values_to_be_between("mesh_size", 0.05, 5000, mostly=0.99)
    if "microplastics_measurement" in df.columns:
        v.expect_column_values_to_be_between("microplastics_measurement", 0, 1e7, mostly=0.99)

    # exigir dominio de 'unit' sólo en PRE
    if "unit" in df.columns and stage_label != "post":
        v.expect_column_values_to_be_in_set("unit", ["items/m3", "items/l", "items/km2"])

    if "doi" in df.columns:
        v.expect_column_values_to_match_regex(
            "doi", r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$", mostly=0.9
        )
    if {"objectid", "date"}.issubset(df.columns):
        v.expect_compound_columns_to_be_unique(["objectid", "date"])
    return v


def _apply_expectations_climate(context, batch_def, df):
    df = df.copy().reset_index(drop=True)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    if "extraction_timestamp" in df.columns:
        df["extraction_timestamp"] = pd.to_datetime(df["extraction_timestamp"], errors="coerce", utc=True)

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    v = context.get_validator(batch=batch)

    req = [
        "date", "latitude", "longitude", "wave_height_max", "wave_period_max",
        "swell_wave_height_max", "wind_wave_direction_dominant"
    ]
    for c in req:
        if c in df.columns:
            v.expect_column_to_exist(c)

    for c in ["date", "latitude", "longitude"]:
        if c in df.columns:
            v.expect_column_values_to_not_be_null(c)

    if "latitude" in df.columns:
        v.expect_column_values_to_be_between("latitude", 5, 83, mostly=0.99)
    if "longitude" in df.columns:
        v.expect_column_values_to_be_between("longitude", -180, -50, mostly=0.99)
    if "wave_height_max" in df.columns:
        v.expect_column_values_to_be_between("wave_height_max", 0, 30, mostly=0.995)
    if "wave_period_max" in df.columns:
        v.expect_column_values_to_be_between("wave_period_max", 0, 25, mostly=0.995)
    if "swell_wave_height_max" in df.columns:
        v.expect_column_values_to_be_between("swell_wave_height_max", 0, 20, mostly=0.995)
    if "wind_wave_direction_dominant" in df.columns:
        v.expect_column_values_to_be_between("wind_wave_direction_dominant", 0, 360, mostly=0.999)

    if "extraction_timestamp" in df.columns:
        current_year = datetime.now().year
        v.expect_column_values_to_be_between(
            "extraction_timestamp",
            pd.Timestamp("2020-01-01", tz="UTC"),
            pd.Timestamp(f"{current_year}-12-31", tz="UTC")
        )
    if "location_id" in df.columns:
        v.expect_compound_columns_to_be_unique(["date", "location_id"])
    else:
        if {"latitude", "longitude", "date"}.issubset(df.columns):
            lat_cell = (df["latitude"] / 0.25).round(0)
            lon_cell = (df["longitude"] / 0.25).round(0)
            df2 = df.assign(_lat_cell=lat_cell, _lon_cell=lon_cell).reset_index(drop=True)
            batch = batch_def.get_batch(batch_parameters={"dataframe": df2})
            v = context.get_validator(batch=batch)
            v.expect_compound_columns_to_be_unique(["date", "_lat_cell", "_lon_cell"])
    return v


def _apply_expectations_species(context, batch_def, df):
    df = df.copy().reset_index(drop=True)
    if "eventDate" in df.columns:
        df["eventDate"] = pd.to_datetime(df["eventDate"], errors="coerce", utc=True)
    if "year" not in df.columns and "eventDate" in df.columns:
        df["year"] = df["eventDate"].dt.year

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    v = context.get_validator(batch=batch)

    req = [
        "gbifID", "eventDate", "year", "decimalLatitude", "decimalLongitude",
        "stateProvince", "locality", "kingdom", "phylum", "class", "order", "family",
        "genus", "species_clean", "basisOfRecord", "countryCode"
    ]
    for c in req:
        if c in df.columns:
            v.expect_column_to_exist(c)

    for c in ["gbifID", "eventDate", "decimalLatitude", "decimalLongitude"]:
        if c in df.columns:
            v.expect_column_values_to_not_be_null(c)

    if "decimalLatitude" in df.columns:
        v.expect_column_values_to_be_between("decimalLatitude", 5, 83)
    if "decimalLongitude" in df.columns:
        v.expect_column_values_to_be_between("decimalLongitude", -180, -50)
    if "year" in df.columns:
        v.expect_column_values_to_be_between("year", 1950, datetime.now().year)

    if "species_clean" in df.columns:
        v.expect_column_values_to_match_regex("species_clean",
                                              r"^[A-Z][a-z]+ [a-z\-]+$",
                                              mostly=0.9)

    if "kingdom" in df.columns:
        v.expect_column_values_to_be_in_set(
            "kingdom", ["Animalia", "Plantae", "Protista", "Fungi", "Chromista", "Bacteria", "Archaea"]
        )
    if "countryCode" in df.columns:
        v.expect_column_values_to_be_in_set("countryCode", ["US", "MX", "CA"], mostly=0.98)
    if "basisOfRecord" in df.columns:
        v.expect_column_values_to_be_in_set(
            "basisOfRecord",
            ["HUMAN_OBSERVATION", "OBSERVATION", "MACHINE_OBSERVATION",
             "PRESERVED_SPECIMEN", "MATERIAL_SAMPLE", "LIVING_SPECIMEN"],
            mostly=0.95
        )
    if "gbifID" in df.columns:
        v.expect_column_values_to_be_unique("gbifID")
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
    # Controles de tamaño (puedes ajustarlos al llamar desde el DAG)
    mp_max_rows: Optional[int] = 200_000,
    cl_max_rows: Optional[int] = 200_000,
    sp_max_rows: Optional[int] = 300_000,
    mp_frac: Optional[float] = None,   # ej. 0.5 para 50%
    cl_frac: Optional[float] = None,
    sp_frac: Optional[float] = None,
):
    """
    Ejecuta validaciones GX para los 3 datasets, imprime resúmenes y guarda un Excel combinado.

    Optimizado para memoria:
    - Proyecta a columnas requeridas por las expectativas (menos RAM).
    - Muestrea filas si exceden umbrales (evita OOM).

    En 'post' NO valida dominio de 'unit' (se mantiene comportamiento original).
    """

    # 1) Proyección de columnas mínimas por dataset
    MP_REQ = [
        "objectid", "date", "latitude", "longitude", "region", "country", "ocean",
        "marine_setting", "microplastics_measurement", "unit", "mesh_size", "doi"
    ]
    CL_REQ = [
        "date", "latitude", "longitude", "wave_height_max", "wave_period_max",
        "swell_wave_height_max", "wind_wave_direction_dominant",
        "location_id", "extraction_timestamp"
    ]
    SP_REQ = [
        "gbifID", "eventDate", "year", "decimalLatitude", "decimalLongitude",
        "stateProvince", "locality", "kingdom", "phylum", "class", "order", "family",
        "genus", "species_clean", "basisOfRecord", "countryCode"
    ]

    microplastics_df = _project(microplastics_df, MP_REQ)
    climate_df       = _project(climate_df,       CL_REQ)
    species_df       = _project(species_df,       SP_REQ)

    # 2) Downsample defensivo (elige frac o max_rows)
    microplastics_df = _downsample(microplastics_df, mp_max_rows, mp_frac)
    climate_df       = _downsample(climate_df,     cl_max_rows, cl_frac)
    species_df       = _downsample(species_df,     sp_max_rows, sp_frac)

    # 3) Idempotencia y assets
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

    # 4) Validaciones
    v_mp = _apply_expectations_microplastics(context, mp_batch_def, microplastics_df, stage_label=stage_label)
    res_mp = v_mp.validate()
    df_mp = _summarize_gx_results(res_mp, "microplastics", stage_label)

    v_cl = _apply_expectations_climate(context, cl_batch_def, climate_df)
    res_cl = v_cl.validate()
    df_cl = _summarize_gx_results(res_cl, "climate", stage_label)

    v_sp = _apply_expectations_species(context, sp_batch_def, species_df)
    res_sp = v_sp.validate()
    df_sp = _summarize_gx_results(res_sp, "species", stage_label)

    # 5) Excel combinado
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
