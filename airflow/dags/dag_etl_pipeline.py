from airflow.decorators import dag, task
from datetime import datetime
import os, sys, subprocess
import pandas as pd

sys.path.append("/opt/airflow/")
sys.path.append("/opt/airflow/scripts")

from scripts.extract import (
    extract_marine_species,
    extract_microplastics,
    extract_marine_climate_from_grids,
    get_climate_history_summary,
)
from scripts.transform import transform_data

DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def _import_ge_runner():
    import importlib
    try:
        gx = importlib.import_module("great_expectations")
        run_fn = importlib.import_module("data_quality_checks_idempotent").run_gx_quality_checks
        return gx, run_fn
    except:
        return None, None

@dag(
    dag_id="etl_ods14_marine_life",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "marine"],
)
def etl():
    @task()
    def extract_microplastics_task():
        path = os.path.join(DATA_DIR, "microplastics.csv")
        if os.path.exists(path):
            return path
        df = extract_microplastics()
        df.to_csv(path, index=False, sep=",", encoding="utf-8")
        return path

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
    def transform_first_pass_task(microplastics_path: str, species_path: str):
        """Genera grids_catalog.csv primero."""
        dummy_climate = os.path.join(DATA_DIR, "dummy_climate.csv")
        pd.DataFrame(columns=["date", "latitude", "longitude"]).to_csv(dummy_climate, index=False)
        
        outputs = transform_data(
            microplastics_path=microplastics_path,
            species_path=species_path,
            climate_path=dummy_climate,
            output_dir=OUTPUT_DIR,
        )
        return outputs.get("grids")

    @task()
    def extract_climate_from_grids_task(grids_path: str):
        """Extrae clima usando las grillas generadas."""
        path = os.path.join(DATA_DIR, "marine_climate_history.csv")
        df = extract_marine_climate_from_grids(
            grids_csv_path=grids_path,
            output_csv_path=path,
            days_per_batch=365,
            earliest_date="2020-01-01"
        )
        get_climate_history_summary(path)
        return path

    @task()
    def validate_pre_task(microplastics_path: str, species_path: str, climate_path: str):
        """Validación PRE con GE (genera CSV)."""
        gx, run_gx = _import_ge_runner()
        if gx is None or run_gx is None:
            print("[WARN] GE no disponible, se omite validación PRE.")
            return None
        
        try:
            ctx = gx.get_context()
            df_micro = pd.read_csv(microplastics_path)
            df_clim = pd.read_csv(climate_path)
            
            try:
                df_spec = pd.read_csv(species_path, sep="\t")
            except:
                df_spec = pd.read_csv(species_path)
            
            _, csv_path = run_gx(
                context=ctx,
                microplastics_df=df_micro,
                climate_df=df_clim,
                species_df=df_spec,
                stage_label="pre",
            )
            
            # CRÍTICO: Guardar también como CSV
            csv_output = os.path.join(DATA_DIR, f"validation_pre_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            df_results = pd.concat([
                pd.read_excel(csv_path, sheet_name="microplastics"),
                pd.read_excel(csv_path, sheet_name="climate"),
                pd.read_excel(csv_path, sheet_name="species")
            ], ignore_index=True)
            df_results.to_csv(csv_output, index=False)
            print(f"[GE] CSV guardado: {csv_output}")
            
            return {"excel": csv_path, "csv": csv_output}
        except Exception as e:
            print(f"[WARN] validate_pre falló: {e}")
            return None

    @task()
    def transform_final_task(microplastics_path: str, species_path: str, climate_path: str):
        """Transform final con clima real."""
        outputs = transform_data(
            microplastics_path=microplastics_path,
            species_path=species_path,
            climate_path=climate_path,
            output_dir=OUTPUT_DIR,
        )
        return outputs

    @task()
    def validate_post_task(outputs):
        """Validación POST con GE (genera CSV)."""
        gx, run_gx = _import_ge_runner()
        if gx is None or run_gx is None:
            print("[WARN] GE no disponible, se omite validación POST.")
            return {"merged": outputs.get("merged")}
        
        try:
            ctx = gx.get_context()
            
            micro_p = outputs.get("microplastics")
            species_p = outputs.get("species")
            climate_p = outputs.get("climate")
            merged_p = outputs.get("merged")
            
            df_micro = pd.read_csv(micro_p)
            df_clim = pd.read_csv(climate_p)
            
            # Species puede estar vacío
            try:
                df_spec = pd.read_csv(species_p)
                if df_spec.empty or df_spec.shape[1] <= 1:
                    df_spec = pd.DataFrame(columns=["gbifID", "decimallatitude", "decimallongitude"])
            except:
                df_spec = pd.DataFrame(columns=["gbifID", "decimallatitude", "decimallongitude"])
            
            _, excel_path = run_gx(
                context=ctx,
                microplastics_df=df_micro,
                climate_df=df_clim,
                species_df=df_spec,
                stage_label="post",
            )
            
            # CRÍTICO: Guardar también como CSV
            csv_output = os.path.join(DATA_DIR, f"validation_post_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            df_results = pd.concat([
                pd.read_excel(excel_path, sheet_name="microplastics"),
                pd.read_excel(excel_path, sheet_name="climate"),
                pd.read_excel(excel_path, sheet_name="species")
            ], ignore_index=True)
            df_results.to_csv(csv_output, index=False)
            print(f"[GE] CSV guardado: {csv_output}")
            
            return {"excel": excel_path, "csv": csv_output, "merged": merged_p}
        except Exception as e:
            print(f"[WARN] validate_post falló: {e}")
            return {"merged": outputs.get("merged")}

    @task()
    def load_to_db_task(validate_output):
        """Carga a MySQL."""
        merged_path = validate_output.get("merged") if isinstance(validate_output, dict) else validate_output
        
        if not os.path.exists(merged_path):
            raise FileNotFoundError(f"No existe: {merged_path}")
        
        result = subprocess.run(
            [sys.executable, "/opt/airflow/scripts/load.py", merged_path],
            capture_output=True,
            text=True
        )
        
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"Carga falló: {result.stderr}")
        
        return "OK"

    # Flujo en 2 fases
    raw_micro = extract_microplastics_task()
    raw_spec = extract_marine_species_task()
    
    # Fase 1: Generar grillas
    grids_path = transform_first_pass_task(raw_micro, raw_spec)
    
    # Fase 2: Extraer clima desde grillas
    climate_path = extract_climate_from_grids_task(grids_path)
    
    # Fase 3: Validación PRE
    pre_report = validate_pre_task(raw_micro, raw_spec, climate_path)
    
    # Fase 4: Transform final
    outputs = transform_final_task(raw_micro, raw_spec, climate_path)
    
    # Fase 5: Validación POST
    post_report = validate_post_task(outputs)
    
    # Fase 6: Load
    load_result = load_to_db_task(post_report)

etl()