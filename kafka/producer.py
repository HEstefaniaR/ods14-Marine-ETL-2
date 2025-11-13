import json
import time
import logging
from datetime import datetime
import os
import pymysql
from pymysql.cursors import DictCursor
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MicroplasticsMetricsProducer:
    def __init__(
        self,
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="microplastics-metrics",
        db_host="localhost",
        db_port=3306,
        db_user="root",
        db_password="root",
        db_name="marineDB",
        interval_seconds=10
    ):
        self.kafka_topic = kafka_topic
        self.interval_seconds = interval_seconds
        self.db_config = {
            "host": db_host,
            "port": db_port,
            "user": db_user,
            "password": db_password,
            "database": db_name,
            "cursorclass": DictCursor
        }

        logger.info(f"Conectando a Kafka ({kafka_bootstrap_servers})...")
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            compression_type="gzip"
        )
        logger.info("Productor Kafka inicializado correctamente")

    def get_db_connection(self):
        return pymysql.connect(**self.db_config)

    def query_db(self, query):
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {e}")
            return []

    def metric_avg_concentration(self):
        return self.query_db("""
            SELECT 
                dl.ocean,
                dl.country,
                AVG(fm.measurement) AS avg_concentration,
                COUNT(*) AS total_observations,
                fm.unit
            FROM fact_microplastics fm
            JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
            WHERE fm.measurement IS NOT NULL
            GROUP BY dl.ocean, dl.country, fm.unit
            ORDER BY avg_concentration DESC
            LIMIT 10;
        """)

    def metric_observations_by_time(self):
        return self.query_db("""
            SELECT 
                dd.year,
                dd.month,
                COUNT(*) AS total_observations,
                AVG(fm.measurement) AS avg_measurement,
                fm.unit
            FROM fact_microplastics fm
            JOIN dim_date dd ON fm.dim_date_date_id = dd.date_id
            WHERE fm.measurement IS NOT NULL
            GROUP BY dd.year, dd.month, fm.unit
            ORDER BY dd.year DESC, dd.month DESC
            LIMIT 12;
        """)

    def metric_max_concentration_climate(self):
        return self.query_db("""
            SELECT 
                CASE 
                    WHEN dc.wave_height_max < 2 THEN 'calm'
                    WHEN dc.wave_height_max BETWEEN 2 AND 4 THEN 'moderate'
                    ELSE 'rough'
                END AS sea_condition,
                MAX(fm.measurement) AS max_concentration,
                AVG(fm.measurement) AS avg_concentration,
                COUNT(*) AS observation_count,
                fm.unit
            FROM fact_microplastics fm
            JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
            WHERE fm.measurement IS NOT NULL
            GROUP BY sea_condition, fm.unit
            ORDER BY max_concentration DESC;
        """)

    def send_metric(self, metric_type, data):
        if not data:
            logger.info(f"No hay datos para {metric_type}")
            return

        message = {
            "metric_type": metric_type,
            "timestamp": datetime.now().isoformat(),
            "data": data,
            "record_count": len(data)
        }

        try:
            future = self.producer.send(self.kafka_topic, key=metric_type, value=message)
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Métrica enviada ({metric_type}) - Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
            )
        except KafkaError as e:
            logger.error(f"Error enviando {metric_type} a Kafka: {e}")

    def run(self):
        logger.info("Iniciando productor de métricas (Ctrl+C para detener)")
        iteration = 0
        try:
            while True:
                iteration += 1
                logger.info(f"Iteración #{iteration}")
                self.send_metric("avg_concentration_by_region", self.metric_avg_concentration())
                self.send_metric("observations_by_timeperiod", self.metric_observations_by_time())
                self.send_metric("max_concentration_by_climate", self.metric_max_concentration_climate())
                self.producer.flush()
                time.sleep(self.interval_seconds)
        except KeyboardInterrupt:
            logger.info("Productor detenido manualmente.")
        finally:
            self.close()

    def close(self):
        logger.info("Cerrando productor...")
        self.producer.close()
        logger.info("Productor cerrado correctamente.")

def main():
    config = {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic": os.getenv("KAFKA_TOPIC", "microplastics-metrics"),
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_port": int(os.getenv("DB_PORT", "3306")),
        "db_user": os.getenv("DB_USER", "root"),
        "db_password": os.getenv("DB_PASSWORD", "root"),
        "db_name": os.getenv("DB_NAME", "marineDB"),
        "interval_seconds": int(os.getenv("INTERVAL_SECONDS", "10")),
    }
    producer = MicroplasticsMetricsProducer(**config)
    producer.run()

if __name__ == "__main__":
    main()