from kafka import KafkaProducer
from faker import Faker
import random
import time
import json
from datetime import datetime

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'iot_sensor_data'

# Initialize Faker with pt_BR locale for Brazilian locations
fake = Faker('pt_BR')

# Global variable to store the latest sensor data
latest_sensor_data = []

# Define sensor types and their fixed attributes
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

def generate_sensor_data():
    """Generate simulated sensor data for all sensor types."""
    timestamp = datetime.now().isoformat()
    
    sensor_data = []
    for sensor_type, attributes in SENSOR_TYPES.items():
        reading = {
            'timestamp': timestamp,
            'sensor_type': sensor_type,
            'value': round(random.uniform(attributes['min_value'], attributes['max_value']), 2),
            'status': random.choice(['active', 'active', 'active', 'maintenance']),  # 75% chance of being active
            'maintenance_date': fake.date_this_year().isoformat() if random.random() < 0.1 else None,  # 10% chance of having maintenance
            **attributes
        }
        sensor_data.append(reading)
    
    return sensor_data

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def simulate_iot_sensors():
    """Simulate continuous IoT sensor data generation and sending to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=json_serializer
    )

    print(f"Starting IoT sensor simulation. Sending data to Kafka topic: {KAFKA_TOPIC}")
    try:
        while True:
            sensor_data = generate_sensor_data()
            for reading in sensor_data:
                producer.send(KAFKA_TOPIC, reading)
                print(f"Sent: {reading['sensor_type']} data to Kafka")
            producer.flush()
            time.sleep(2)  # Generate new readings every 2 seconds
    except KeyboardInterrupt:
        print("\nSimulation stopped by user")
    finally:
        producer.close()

if __name__ == "__main__":
    print("Starting IoT Sensor Simulator...")
    simulate_iot_sensors()
