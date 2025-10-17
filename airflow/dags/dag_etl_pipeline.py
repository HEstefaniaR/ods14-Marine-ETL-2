from airflow.decorators import dag, task
from datetime import datetime
import sys
import os
import pandas as pd
from typing import List

sys.path.append("/opt/airflow/")

from scripts.extract import extract_marine_species, extract_microplastics
DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@dag(
    dag_id="etl_ods14_marine_life",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "airflow", "api"],
)
def etl():
    @task()
    def extract_marine_species_task():
        df = extract_marine_species()
        output_path = os.path.join(DATA_DIR, "marine_species.csv")
        df.to_csv(output_path, index=False)
        print(f"Archivo guardado en {output_path}")
        return output_path 
    
    @task()
    def extract_microplastics_task():
        df = extract_microplastics()
        output_path = os.path.join(DATA_DIR, "microplastics.csv")
        df.to_csv(output_path, index=False)
        print(f"Archivo guardado en {output_path}")
        return output_path
    
    # MySQL
    raw_microplastics = extract_microplastics_task()

    # .csv
    raw_marine_species = extract_marine_species_task()

    # API



etl()