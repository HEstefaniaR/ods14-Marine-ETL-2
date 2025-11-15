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
        interval_seconds=5
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
        logger.info("Kafka producer listo.")

    def get_db_connection(self):
        return pymysql.connect(**self.db_config)

    def query_db(self, query, params=None):
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"SQL error: {e}")
            return []

    def get_years(self):
        years = self.query_db("""
            SELECT DISTINCT year
            FROM dim_date
            ORDER BY year ASC;
        """)
        return [row["year"] for row in years]

    def metric_avg_concentration(self, year):
        return self.query_db("""
            SELECT 
                dl.ocean AS ocean_region,
                dl.marine_setting AS environment_type,
                AVG(fm.measurement) AS avg_concentration,
                COUNT(*) AS total_observations,
                fm.unit
            FROM fact_microplastics fm
            JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
            JOIN dim_date dd ON fm.dim_date_date_id = dd.date_id
            WHERE dd.year = %s
            GROUP BY dl.ocean, dl.marine_setting, fm.unit
            ORDER BY avg_concentration DESC;
        """, (year,))
    
    def metric_ecological_risk_raw(self, year):
        return self.query_db("""
            SELECT 
                dl.ocean AS ocean_region,
                dl.marine_setting AS environment_type,
                COUNT(*) AS total_microplastics,
                AVG(fm.measurement) AS avg_pollution,
                COUNT(DISTINCT msb.dim_species_species_id) AS species_count,
                (AVG(fm.measurement) * COUNT(DISTINCT msb.dim_species_species_id)) AS risk_raw
            FROM fact_microplastics fm
            JOIN dim_location dl ON dl.location_id = fm.dim_location_location_id
            JOIN dim_date dd ON dd.date_id = fm.dim_date_date_id
            LEFT JOIN microplastics_species_bridge msb 
                   ON msb.fact_microplastics_observation_id = fm.observation_id
            WHERE dd.year = %s
            GROUP BY dl.ocean, dl.marine_setting
            ORDER BY risk_raw DESC;
        """, (year,))

    def metric_ecological_risk(self, year):
        raw = self.metric_ecological_risk_raw(year)
        if not raw:
            return []

        vals = [r["risk_raw"] for r in raw]
        mn, mx = min(vals), max(vals)
        if mx == mn:
            for r in raw:
                r["risk_index"] = 0
            return raw

        for r in raw:
            r["risk_index"] = 100 * (r["risk_raw"] - mn) / (mx - mn)

        return raw

    def send_metric(self, metric_type, year, data):
        message = {
            "metric_type": metric_type,
            "year": year,
            "timestamp": datetime.now().isoformat(),
            "data": data,
            "record_count": len(data)
        }

        try:
            fut = self.producer.send(
                self.kafka_topic, key=metric_type, value=message
            )
            meta = fut.get(timeout=10)
            logger.info(f"Sent {metric_type} year={year} offset={meta.offset}")
        except KafkaError as e:
            logger.error(f"Kafka send error: {e}")


    def run(self):
        logger.info("Producer ON (annual only).")

        try:
            while True:
                years = self.get_years()
                logger.info(f"Años detectados: {years}")

                for year in years:
                    logger.info(f"Producing metrics for year {year}")

                    self.send_metric("avg_concentration_by_region", year, self.metric_avg_concentration(year))
                    self.send_metric("ecological_risk_index", year, self.metric_ecological_risk(year))

                    time.sleep(self.interval_seconds)

                logger.info("Ciclo completo. Reiniciando desde el primer año...")

        except KeyboardInterrupt:
            logger.info("Producer detenido manualmente.")

        finally:
            self.close()

    def close(self):
        logger.info("Cerrando productor Kafka…")
        self.producer.close()


def main():
    config = {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic": os.getenv("KAFKA_TOPIC", "microplastics-metrics"),
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_port": int(os.getenv("DB_PORT", "3306")),
        "db_user": os.getenv("DB_USER", "root"),
        "db_password": os.getenv("DB_PASSWORD", "root"),
        "db_name": os.getenv("DB_NAME", "marineDB"),
        "interval_seconds": int(os.getenv("INTERVAL_SECONDS", "5")),
    }
    MicroplasticsMetricsProducer(**config).run()


if __name__ == "__main__":
    main()