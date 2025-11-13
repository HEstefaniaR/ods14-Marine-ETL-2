import json
import logging
from datetime import datetime
from collections import deque
import os
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MicroplasticsMetricsConsumer:
    def __init__(
        self,
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="microplastics-metrics",
        group_id="microplastics-consumer-group",
        max_messages_buffer=100
    ):
        self.kafka_topic = kafka_topic
        self.group_id = group_id
        self.messages_buffer = deque(maxlen=max_messages_buffer)
        self.summary = {"total_messages": 0, "by_type": {}, "last_update": None}

        logger.info(f"Conectando a Kafka ({kafka_bootstrap_servers}) en topic '{kafka_topic}'")
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
        logger.info("Consumidor Kafka inicializado correctamente")

    def process_message(self, message):
        value = message.value
        key = message.key
        metric_type = value.get("metric_type", "unknown")

        self.summary["total_messages"] += 1
        self.summary["last_update"] = datetime.now().isoformat()
        self.summary["by_type"][metric_type] = self.summary["by_type"].get(metric_type, 0) + 1

        self.messages_buffer.append({
            "key": key,
            "value": value,
            "timestamp": datetime.now().isoformat(),
            "partition": message.partition,
            "offset": message.offset
        })

        print(f"\n{'='*70}")
        print(f"Métrica recibida: {metric_type}")
        print(f"Timestamp: {value.get('timestamp')}")
        print(f"Registros: {len(value.get('data', []))}")
        print(f"Partition: {message.partition} | Offset: {message.offset}")
        print(f"{'='*70}")

        logger.info(f"Mensaje procesado ({metric_type}) - offset {message.offset}")

    def print_summary(self):
        print("\n" + "="*70)
        print("Resumen del consumidor")
        print("="*70)
        print(f"Total de mensajes: {self.summary['total_messages']}")
        print(f"Última actualización: {self.summary['last_update']}")
        for t, c in self.summary["by_type"].items():
            print(f"  {t}: {c}")
        print("="*70)

    def run(self):
        logger.info("Esperando mensajes de Kafka")
        try:
            for msg in self.consumer:
                self.process_message(msg)
                if self.summary["total_messages"] % 10 == 0:
                    self.print_summary()
        except KeyboardInterrupt:
            logger.info("Consumidor detenido manualmente.")
        except KafkaError as e:
            logger.error(f"Error de Kafka: {e}")
        finally:
            self.close()

    def close(self):
        logger.info("Cerrando consumidor...")
        self.print_summary()
        self.consumer.close()
        logger.info("Consumidor cerrado correctamente.")

def main():
    config = {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic": os.getenv("KAFKA_TOPIC", "microplastics-metrics"),
        "group_id": os.getenv("CONSUMER_GROUP_ID", "microplastics-consumer-group"),
    }
    consumer = MicroplasticsMetricsConsumer(**config)
    consumer.run()

if __name__ == "__main__":
    main()