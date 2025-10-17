import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

def extract_marine_species(csv_path="/opt/airflow/raw_data/marine_species.csv") -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path, sep="\t", quoting=3, on_bad_lines="skip", low_memory=False)
        print(f"'Marine Species' TSV cargado correctamente: {df.shape[0]} filas y {df.shape[1]} columnas")
        return df
    except Exception as e:
        print(f"Error al leer 'Marine Species' TSV: {e}")
        raise

def extract_microplastics(mysql_conn_str="mysql+pymysql://root:root@host.docker.internal:3306/microplastics_db",
                   table_name="microplastics") -> pd.DataFrame:
    try:
        engine = create_engine(mysql_conn_str)
        df = pd.read_sql_table(table_name, con=engine)
        print(f"Tabla 'microplastics' cargada correctamente: {df.shape[0]} filas")
        return df
    except Exception as e:
        print(f"Error al extraer la tabla 'microplastics' desde MySQL: {e}")
        raise

