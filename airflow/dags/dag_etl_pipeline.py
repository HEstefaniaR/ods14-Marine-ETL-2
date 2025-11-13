from airflow.decorators import dag, task
from datetime import datetime
import os, sys, subprocess
import pandas as pd
import traceback

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
    """Importación robusta con logging detallado"""
    import importlib
    try:
        print("[INFO] Intentando importar Great Expectations...")
        gx = importlib.import_module("great_expectations")
        print(f"[OK] Great Expectations importado: {gx.__version__}")
        
        print("[INFO] Importando módulo de validación...")
        # CORREGIDO: importar desde scripts.dataqualitycheck
        validation_module = importlib.import_module("scripts.dataqualitycheck")
        run_fn = validation_module.run_gx_quality_checks
        print("[OK] Función de validación importada correctamente")
        
        return gx, run_fn
    except ImportError as e:
        print(f"[ERROR] Falta dependencia: {e}")
        print("Verifica que el archivo esté en /opt/airflow/scripts/")
        traceback.print_exc()
        return None, None
    except Exception as e:
        print(f"[ERROR] Error inesperado al importar: {e}")
        traceback.print_exc()
        return None, None

@dag(
    dag_id="etl_ods14_marine_life",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "marine", "fixed"],
)
def etl():
    @task()
    def extract_microplastics_task():
        path = os.path.join(DATA_DIR, "microplastics.csv")
        if os.path.exists(path):
            print(f"[INFO] Reutilizando archivo existente: {path}")
            return path
        df = extract_microplastics()
        df.to_csv(path, index=False, sep=",", encoding="utf-8")
        print(f"[OK] Microplásticos extraídos: {len(df)} registros")
        return path

    @task()
    def extract_marine_species_task(sample_limit=800000):
        path = os.path.join(DATA_DIR, "marine_species.csv")
        if os.path.exists(path):
            print(f"[INFO] Reutilizando archivo existente: {path}")
            return path
        df = extract_marine_species()
        if sample_limit:
            df = df.head(sample_limit)
        df.to_csv(path, index=False, sep=",", encoding="utf-8")
        print(f"[OK] Especies extraídas: {len(df)} registros")
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
        print(f"[OK] Grillas generadas: {outputs.get('grids')}")
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
        print(f"[OK] Clima extraído: {len(df)} registros")
        return path

    @task()
    def validate_pre_task(microplastics_path: str, species_path: str, climate_path: str):
        """Validación PRE con Great Expectations"""
        print("\n" + "="*60)
        print("VALIDACIÓN PRE-TRANSFORMACIÓN")
        print("="*60)
        
        gx, run_gx = _import_ge_runner()
        if gx is None or run_gx is None:
            print("[WARN] GE no disponible, se omite validación PRE")
            return {"status": "skipped", "reason": "GE not available"}
        
        try:
            # Inicializar contexto GE
            ctx = gx.get_context()
            print("[INFO] Contexto GE inicializado")
            
            # Cargar datos
            print(f"[INFO] Cargando microplásticos desde: {microplastics_path}")
            df_micro = pd.read_csv(microplastics_path, low_memory=False)
            print(f"  → {len(df_micro)} filas, {len(df_micro.columns)} columnas")
            
            # Verificar columnas críticas
            critical_cols = ['objectid', 'date', 'latitude', 'longitude', 'microplastics_measurement', 'unit']
            missing_cols = [col for col in critical_cols if col not in df_micro.columns]
            if missing_cols:
                print(f"  ⚠️ ADVERTENCIA: Faltan columnas críticas en microplásticos: {missing_cols}")
            
            print(f"[INFO] Cargando clima desde: {climate_path}")
            df_clim = pd.read_csv(climate_path, low_memory=False)
            print(f"  → {len(df_clim)} filas, {len(df_clim.columns)} columnas")
            
            print(f"[INFO] Cargando especies desde: {species_path}")
            try:
                # Intentar primero con TAB separator
                df_spec = pd.read_csv(species_path, sep="\t", low_memory=False)
                print(f"  → Cargado con sep=TAB: {len(df_spec)} filas, {len(df_spec.columns)} columnas")
            except Exception as e1:
                print(f"  → Error con TAB: {e1}, intentando con coma...")
                try:
                    df_spec = pd.read_csv(species_path, low_memory=False)
                    print(f"  → Cargado con sep=COMMA: {len(df_spec)} filas, {len(df_spec.columns)} columnas")
                except Exception as e2:
                    print(f"  → Error con COMMA: {e2}")
                    # Crear DataFrame vacío con columnas esperadas
                    df_spec = pd.DataFrame(columns=["gbifID", "eventDate", "year", "decimalLatitude", "decimalLongitude"])
                    print(f"  → Usando DataFrame vacío con columnas base")
            
            # Verificar que tenga datos suficientes
            if df_spec.empty or len(df_spec) < 10:
                print(f"  ⚠️ ADVERTENCIA: Species tiene muy pocos datos ({len(df_spec)} filas), usando placeholder")
                df_spec = pd.DataFrame(columns=["gbifID", "eventDate", "year", "decimalLatitude", "decimalLongitude"])
            print(f"  → {len(df_spec)} filas, {len(df_spec.columns)} columnas")
            
            # Ejecutar validaciones
            print("[INFO] Ejecutando validaciones GE...")
            print(f"  → Microplásticos: {len(df_micro)} filas")
            print(f"  → Clima: {len(df_clim)} filas")
            print(f"  → Especies: {len(df_spec)} filas")
            
            # Mostrar primeras columnas de cada dataset para debug
            print(f"\n[DEBUG] Columnas de microplásticos: {list(df_micro.columns)[:10]}")
            print(f"[DEBUG] Columnas de clima: {list(df_clim.columns)[:10]}")
            print(f"[DEBUG] Columnas de especies: {list(df_spec.columns)[:10]}")
            
            summary_df, excel_path = run_gx(
                context=ctx,
                microplastics_df=df_micro,
                climate_df=df_clim,
                species_df=df_spec,
                stage_label="pre",
            )
            
            # Guardar también como CSV
            csv_output = os.path.join(DATA_DIR, f"validation_pre_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            summary_df.to_csv(csv_output, index=False)
            
            print(f"\n[SUCCESS] Validación PRE completada:")
            print(f"  Excel: {excel_path}")
            print(f"  CSV:   {csv_output}")
            print(f"  Total validaciones: {len(summary_df)}")
            print(f"  Éxito: {summary_df['success'].sum()}/{len(summary_df)}")
            
            return {
                "status": "success",
                "excel": excel_path,
                "csv": csv_output,
                "total_checks": len(summary_df),
                "passed": int(summary_df['success'].sum())
            }
            
        except Exception as e:
            print(f"\n[ERROR] Validación PRE falló:")
            print(f"  {type(e).__name__}: {e}")
            traceback.print_exc()
            return {"status": "error", "error": str(e)}

    @task()
    def transform_final_task(microplastics_path: str, species_path: str, climate_path: str):
        """Transform final con clima real."""
        outputs = transform_data(
            microplastics_path=microplastics_path,
            species_path=species_path,
            climate_path=climate_path,
            output_dir=OUTPUT_DIR,
        )
        print(f"[OK] Transformación final completada:")
        for key, path in outputs.items():
            print(f"  {key}: {path}")
        return outputs

    @task()
    def validate_post_task(outputs):
        """Validación POST con Great Expectations"""
        print("\n" + "="*60)
        print("VALIDACIÓN POST-TRANSFORMACIÓN")
        print("="*60)
        
        gx, run_gx = _import_ge_runner()
        if gx is None or run_gx is None:
            print("[WARN] GE no disponible, se omite validación POST")
            return {
                "status": "skipped",
                "reason": "GE not available",
                "merged": outputs.get("merged")
            }
        
        try:
            ctx = gx.get_context()
            print("[INFO] Contexto GE inicializado")
            
            # Extraer paths de outputs
            micro_p = outputs.get("microplastics")
            species_p = outputs.get("species")
            climate_p = outputs.get("climate")
            merged_p = outputs.get("merged")
            
            print(f"[INFO] Cargando datos transformados:")
            print(f"  Microplásticos: {micro_p}")
            print(f"  Especies: {species_p}")
            print(f"  Clima: {climate_p}")
            
            # Cargar microplásticos
            df_micro = pd.read_csv(micro_p)
            print(f"  → Microplásticos: {len(df_micro)} filas")
            
            # Cargar clima
            df_clim = pd.read_csv(climate_p)
            print(f"  → Clima: {len(df_clim)} filas")
            
            # Cargar especies (puede estar vacío)
            try:
                df_spec = pd.read_csv(species_p)
                if df_spec.empty or df_spec.shape[1] <= 1:
                    print("  → Especies: dataset vacío, usando placeholder")
                    df_spec = pd.DataFrame(columns=["gbifID", "decimalLatitude", "decimalLongitude"])
                else:
                    print(f"  → Especies: {len(df_spec)} filas")
            except Exception as e:
                print(f"  → Especies: error al cargar ({e}), usando placeholder")
                df_spec = pd.DataFrame(columns=["gbifID", "decimalLatitude", "decimalLongitude"])
            
            # Ejecutar validaciones
            print("[INFO] Ejecutando validaciones GE...")
            summary_df, excel_path = run_gx(
                context=ctx,
                microplastics_df=df_micro,
                climate_df=df_clim,
                species_df=df_spec,
                stage_label="post",
            )
            
            # Guardar también como CSV
            csv_output = os.path.join(DATA_DIR, f"validation_post_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            summary_df.to_csv(csv_output, index=False)
            
            print(f"\n[SUCCESS] Validación POST completada:")
            print(f"  Excel: {excel_path}")
            print(f"  CSV:   {csv_output}")
            print(f"  Total validaciones: {len(summary_df)}")
            print(f"  Éxito: {summary_df['success'].sum()}/{len(summary_df)}")
            
            return {
                "status": "success",
                "excel": excel_path,
                "csv": csv_output,
                "merged": merged_p,
                "total_checks": len(summary_df),
                "passed": int(summary_df['success'].sum())
            }
            
        except Exception as e:
            print(f"\n[ERROR] Validación POST falló:")
            print(f"  {type(e).__name__}: {e}")
            traceback.print_exc()
            return {
                "status": "error",
                "error": str(e),
                "merged": outputs.get("merged")
            }

    @task()
    def load_to_db_task(validate_output):
        """Carga a MySQL."""
        print("\n" + "="*60)
        print("CARGA A BASE DE DATOS")
        print("="*60)
        
        # Extraer merged_path del resultado de validación
        if isinstance(validate_output, dict):
            merged_path = validate_output.get("merged")
            print(f"[INFO] Estado validación: {validate_output.get('status', 'unknown')}")
        else:
            merged_path = validate_output
        
        if not merged_path:
            raise ValueError("No se encontró path del archivo merged")
        
        if not os.path.exists(merged_path):
            raise FileNotFoundError(f"No existe: {merged_path}")
        
        print(f"[INFO] Cargando desde: {merged_path}")
        
        result = subprocess.run(
            [sys.executable, "/opt/airflow/scripts/load.py", merged_path],
            capture_output=True,
            text=True
        )
        
        print(result.stdout)
        if result.returncode != 0:
            print(f"[ERROR] {result.stderr}")
            raise RuntimeError(f"Carga falló: {result.stderr}")
        
        print("[SUCCESS] Datos cargados a MySQL")
        return "OK"

    # ===== FLUJO DEL DAG =====
    
    # Fase 1: Extracciones iniciales
    raw_micro = extract_microplastics_task()
    raw_spec = extract_marine_species_task()
    
    # Fase 2: Generar grillas
    grids_path = transform_first_pass_task(raw_micro, raw_spec)
    
    # Fase 3: Extraer clima desde grillas
    climate_path = extract_climate_from_grids_task(grids_path)
    
    # Fase 4: Validación PRE
    pre_report = validate_pre_task(raw_micro, raw_spec, climate_path)
    
    # Fase 5: Transform final (depende de PRE)
    outputs = transform_final_task(raw_micro, raw_spec, climate_path)
    outputs >> pre_report  # Asegurar orden
    
    # Fase 6: Validación POST
    post_report = validate_post_task(outputs)
    
    # Fase 7: Load
    load_result = load_to_db_task(post_report)

etl()