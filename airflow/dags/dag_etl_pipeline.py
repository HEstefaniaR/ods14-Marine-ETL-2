# /opt/airflow/dags/dag_etl_pipeline.py
from airflow.decorators import dag, task
from datetime import datetime
import os, sys
import pandas as pd
import subprocess

# --- rutas para importar scripts personalizados ---
sys.path.append("/opt/airflow/")
sys.path.append("/opt/airflow/scripts")

from scripts.extract import (
    extract_marine_species,
    extract_microplastics,
    extract_marine_climate_data,
    get_climate_history_summary,
)
from scripts.transform import transform_data, normalize_columns


DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _import_ge_runner():
    """Carga Great Expectations dinámicamente."""
    import importlib
    try:
        gx = importlib.import_module("great_expectations")
    except Exception:
        gx = None
    run_fn = None
    try:
        run_fn = importlib.import_module("data_quality_checks_idempotent").run_gx_quality_checks
    except Exception:
        try:
            run_fn = importlib.import_module("dataqualitycheck").run_gx_quality_checks
        except Exception:
            pass
    return gx, run_fn


@dag(
    dag_id="etl_ods14_marine_life",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "marine", "airflow"],
)
def etl():
    # ------------ Extract ------------
    @task()
    def extract_marine_species_task():
        df = extract_marine_species()
        p = os.path.join(DATA_DIR, "marine_species.csv")
        df.to_csv(p, index=False, sep=",", encoding="utf-8")
        return p

    @task()
    def extract_microplastics_task():
        df = extract_microplastics()
        p = os.path.join(DATA_DIR, "microplastics.csv")
        df.to_csv(p, index=False, sep=",", encoding="utf-8")
        return p

    @task()
    def extract_climate_data_task():
        history_csv_path = os.path.join(DATA_DIR, "marine_climate_history.csv")
        df_new = extract_marine_climate_data(
            output_csv_path=history_csv_path,
            days_per_batch=30,
            earliest_date="1972-01-01",
        )
        get_climate_history_summary(history_csv_path)
        df_new.to_csv(history_csv_path, index=False, sep=",", encoding="utf-8")
        return history_csv_path

    # ------------ Validate (pre) ------------
    @task()
    def validate_pre_task(microplastics_path: str, species_path: str, climate_path: str):
        gx, run_gx = _import_ge_runner()
        if gx is None or run_gx is None:
            print("GE no disponible: se omite validate_pre.")
            return None

        ctx = gx.get_context()
        df_micro = normalize_columns(pd.read_csv(microplastics_path))
        df_spec  = pd.read_csv(species_path)
        df_clim  = pd.read_csv(climate_path)

        _, excel_path = run_gx(
            context=ctx,
            microplastics_df=df_micro,
            climate_df=df_clim,
            species_df=df_spec,
            stage_label="pre",
        )
        return excel_path

    # ------------ Transform ------------
    @task()
    def transform_task(microplastics_path: str, species_path: str, climate_path: str):
        return transform_data(
            microplastics_path=microplastics_path,
            species_path=species_path,
            climate_path=climate_path,
            output_dir=OUTPUT_DIR,
        )

    # ------------ Validate (post) ------------
    @task()
    def validate_post_task(outputs):
        gx, run_gx = _import_ge_runner()
        if gx is None or run_gx is None:
            print("GE no disponible: se omite validate_post.")
            return None

        def _pick_paths(outputs_obj):
            if isinstance(outputs_obj, dict):
                micro = outputs_obj.get("microplastics")
                spec  = outputs_obj.get("species")
                clim  = outputs_obj.get("climate")
                merged = outputs_obj.get("merged")
            else:
                micro = os.path.join(OUTPUT_DIR, "microplastics_clean.csv")
                spec  = os.path.join(OUTPUT_DIR, "marine_species_clean.csv")
                clim  = os.path.join(OUTPUT_DIR, "climate_clean.csv")
                merged = os.path.join(OUTPUT_DIR, "merged_marine_data.csv")
            return micro, spec, clim, merged

        micro_p, species_p, climate_p, merged_p = _pick_paths(outputs)
        print(f"[post] microplastics path: {micro_p}")
        print(f"[post] species path:      {species_p}")
        print(f"[post] climate path:      {climate_p}")
        print(f"[post] merged path (info): {merged_p}")

        # Chequeos de existencia
        missing = [p for p in [micro_p, species_p, climate_p] if not os.path.isfile(p)]
        if missing:
            raise FileNotFoundError(f"[post] Faltan archivos esperados: {missing}")

        ctx = gx.get_context()
        df_micro = pd.read_csv(micro_p)
        df_spec  = pd.read_csv(species_p)
        df_clim  = pd.read_csv(climate_p)

        _, excel_path = run_gx(
            context=ctx,
            microplastics_df=df_micro,
            climate_df=df_clim,
            species_df=df_spec,
            stage_label="post",
        )
        return {"report": excel_path, "merged": merged_p}

    # ------------ Load to DB ------------
    @task()
    def load_to_db_task(validate_output: dict):
        """Ejecuta el script load_star_schema.py para cargar datos en marineDB"""
        merged_csv = validate_output.get("merged") if isinstance(validate_output, dict) else None
        merged_csv = merged_csv or os.path.join(OUTPUT_DIR, "merged_marine_data.csv")
        if not os.path.exists(merged_csv):
            raise FileNotFoundError(f"No existe {merged_csv}. Ejecuta transform_task primero.")

        script_path = "/opt/airflow/scripts/load.py"
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"No existe {script_path}")

        print(f"[LOAD] Ejecutando carga a BD desde {merged_csv} ...")
        result = subprocess.run(["python", script_path], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError("Falló la carga a base de datos (ver logs).")
        print("[LOAD] Carga completada exitosamente.")
        return "Carga completada en marineDB"

    # --- Encadenamiento del flujo ---
    raw_micro = extract_microplastics_task()
    raw_spec = extract_marine_species_task()
    raw_clim = extract_climate_data_task()

    pre_report = validate_pre_task(raw_micro, raw_spec, raw_clim)
    outputs = transform_task(raw_micro, raw_spec, raw_clim)
    post_report = validate_post_task(outputs)
    load_result = load_to_db_task(post_report)


etl()
