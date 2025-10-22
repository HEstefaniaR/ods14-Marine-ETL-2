import pandas as pd
import mysql.connector
import numpy as np
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_CSV = PROJECT_ROOT / "data" / "raw_data" / "marine_microplastics.csv"

config = {
    "user": "root",
    "password": "root",
    "host": "localhost",
    "port": 3306
}

DB_NAME = "microplastics_db"
TABLE_NAME = "microplastics"

conn = mysql.connector.connect(**config)
cursor = conn.cursor()

cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
cursor.execute(f"USE {DB_NAME}")

# df = pd.read_csv("./data/raw_data/marine_microplastics.csv")
df = pd.read_csv(RAW_CSV)


def map_dtype(dtype):
    if np.issubdtype(dtype, np.integer):
        return "INT"
    elif np.issubdtype(dtype, np.floating):
        return "FLOAT"
    elif np.issubdtype(dtype, np.bool_):
        return "BOOLEAN"
    elif np.issubdtype(dtype, np.datetime64):
        return "DATETIME"
    else:
        return "TEXT"

cols = ", ".join([f"`{col}` {map_dtype(dtype)}" for col, dtype in zip(df.columns, df.dtypes)])
cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({cols})")

for _, row in df.iterrows():
    row = [None if pd.isna(x) else x for x in row]
    placeholders = ", ".join(["%s"] * len(row))
    sql = f"INSERT INTO {TABLE_NAME} VALUES ({placeholders})"
    cursor.execute(sql, tuple(row))
    
conn.commit()
cursor.close()
conn.close()