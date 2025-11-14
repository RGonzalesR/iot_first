import os
import logging
from kafka import KafkaProducer
from faker import Faker
import random
import time
import json
from datetime import datetime

from tools.logger_utils import setup_rotating_log

logger = logging.getLogger("producer")

# =========================
# Constantes e parâmetros
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:1111")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot")

fake = Faker('pt_BR')

SENSOR_TYPES = {
    'temperature': {
        'unit': 'Celsius',
        'min_value': -10,
        'max_value': 40,
        'location': fake.city(),
        'sensor_id': fake.uuid4(),
        'manufacturer': fake.company(),
        'model': fake.bothify(text='TEMP-###??'),
        'installation_date': fake.date_this_year().isoformat()
    },
    'humidity': {
        'unit': '%',
        'min_value': 0,
        'max_value': 100,
        'location': fake.city(),
        'sensor_id': fake.uuid4(),
        'manufacturer': fake.company(),
        'model': fake.bothify(text='HUM-###??'),
        'installation_date': fake.date_this_year().isoformat()
    },
    'pressure': {
        'unit': 'hPa',
        'min_value': 900,
        'max_value': 1100,
        'location': fake.city(),
        'sensor_id': fake.uuid4(),
        'manufacturer': fake.company(),
        'model': fake.bothify(text='PRES-###??'),
        'installation_date': fake.date_this_year().isoformat()
    }
}

# =========================
# Funções principais
# =========================
def generate_sensor_data():
    """Geração de dados simulados para todos os tipos de sensores."""
    timestamp = datetime.now().isoformat()
    sensor_data = []

    for sensor_type, attributes in SENSOR_TYPES.items():
        reading = {
            'timestamp': timestamp,
            'sensor_type': sensor_type,
            'value': round(random.uniform(attributes['min_value'] - attributes['min_value'] * 0.2, 
                                          attributes['max_value'] * 1.5), 
                           2),
            'status': random.choice(['active', 'active', 'active', 'maintenance']),                     # 75% ativo
            'maintenance_date': fake.date_this_year().isoformat() if random.random() < 0.1 else None,   # 10% chance de já ter ocorrido manutenção
            **attributes
        }
        sensor_data.append(reading)

    return sensor_data


def json_serializer(data):
    """Serializa dados para JSON."""
    return json.dumps(data).encode('utf-8')


def simulate_iot_sensors():
    """Simula continuamente sensores IoT e envia dados para o Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=json_serializer
    )

    logger.info("=" * 60)
    logger.info("Iniciando simulação de dados de sensores IoT. Pressione CTRL+C para finalizar.")
    logger.info(f"Enviando dados para o tópico Kafka: {KAFKA_TOPIC}")
    logger.info("=" * 60)

    try:
        while True:
            sensor_data = generate_sensor_data()
            for reading in sensor_data:
                producer.send(KAFKA_TOPIC, reading)
                logger.info(f"--> Enviado dado para o Kafka: {reading['sensor_type']} | valor={reading['value']}{reading['unit']}")
            producer.flush()
            time.sleep(2)
    except KeyboardInterrupt:
        logger.info("=" * 60)
        logger.info("Simulação encerrada pelo usuário.")
        logger.info("=" * 60)
    except Exception as e:
        logger.error("Erro inesperado na simulação IoT.", exc_info=True)
    finally:
        producer.close()
        logger.info("Conexão com Kafka encerrada.")


if __name__ == "__main__":
    logger = setup_rotating_log(
        component="producer",
        default_dir="/app/volumes/kafka/logs"
    )

    logger.info("=" * 60)
    logger.info("Iniciando simulador IoT...")
    logger.info("=" * 60)

    simulate_iot_sensors()
