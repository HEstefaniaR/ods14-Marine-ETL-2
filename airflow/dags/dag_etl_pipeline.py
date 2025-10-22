from airflow.decorators import dag, task
from datetime import datetime
import os, sys, time, subprocess
import pandas as pd

sys.path.append("/opt/airflow/")
sys.path.append("/opt/airflow/scripts")

from scripts.extract import (
    extract_marine_species,
    extract_microplastics,
    extract_marine_climate_data,
    get_climate_history_summary,
)
from scripts.transform import transform_data

DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@dag(
    dag_id="etl_ods14_marine_life",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "marine", "airflow", "optimized"],
)
def etl():
    @task()
    def extract_marine_species_task(sample_limit=800000):
        path = os.path.join(DATA_DIR, "marine_species.csv")
        if os.path.exists(path):
            return path
        df = extract_marine_species()
        if sample_limit:
            df = df.head(sample_limit)
        df.to_csv(path, index=False, sep=",", encoding="utf-8")
        return path

    @task()
    def extract_microplastics_task():
        path = os.path.join(DATA_DIR, "microplastics.csv")
        if os.path.exists(path):
            return path
        df = extract_microplastics()
        df.to_csv(path, index=False, sep=",", encoding="utf-8")
        return path

    @task()
    def extract_climate_data_task():
        path = os.path.join(DATA_DIR, "marine_climate_history.csv")
        df_new = extract_marine_climate_data(
            output_csv_path=path,
            days_per_batch=30,
            earliest_date="1972-01-01",
        )
        get_climate_history_summary(path)
        df_new.to_csv(path, index=False, sep=",", encoding="utf-8")
        return path

    @task()
    def validate_pre_task(microplastics_path: str, species_path: str, climate_path: str):
        return None

    @task()
    def transform_task(microplastics_path: str, species_path: str, climate_path: str):
        merged_path = os.path.join(OUTPUT_DIR, "merged_marine_data.csv")
        if os.path.exists(merged_path):
            return {
                "microplastics": os.path.join(OUTPUT_DIR, "microplastics_clean.csv"),
                "species": os.path.join(OUTPUT_DIR, "marine_species_clean.csv"),
                "climate": os.path.join(OUTPUT_DIR, "climate_clean.csv"),
                "merged": merged_path,
            }
        outputs = transform_data(
            microplastics_path=microplastics_path,
            species_path=species_path,
            climate_path=climate_path,
            output_dir=OUTPUT_DIR,
        )
        return outputs

    @task()
    def validate_post_task(outputs):
        merged = outputs.get("merged") if isinstance(outputs, dict) else os.path.join(OUTPUT_DIR, "merged_marine_data.csv")
        return {"report": None, "merged": merged}

    @task()
    def load_to_db_task(validate_output: dict):
        merged_csv = validate_output.get("merged") if isinstance(validate_output, dict) else None
        merged_csv = merged_csv or os.path.join(OUTPUT_DIR, "merged_marine_data.csv")
        if not os.path.exists(merged_csv):
            raise FileNotFoundError(f"No existe {merged_csv}. Ejecuta transform_task primero.")
        script_path = "/opt/airflow/scripts/load.py"
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"No existe {script_path}")
        result = subprocess.run(["python", script_path], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError("Falló la carga a base de datos")
        return "Carga completada en marineDB"

    raw_micro = extract_microplastics_task()
    raw_spec = extract_marine_species_task()
    raw_clim = extract_climate_data_task()

    pre_report = validate_pre_task(raw_micro, raw_spec, raw_clim)
    outputs = transform_task(raw_micro, raw_spec, raw_clim)
    post_report = validate_post_task(outputs)
    load_result = load_to_db_task(post_report)

etl()