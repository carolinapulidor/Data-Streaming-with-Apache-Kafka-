# producer.py
import json
import time
import random
from kafka import KafkaProducer

# config Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Obtain meteorology randomly data
def generate_random_weather_data(city):
    return {
        'city': city,
        'temperature': round(random.uniform(-20, 30), 2),  
        'humidity': random.randint(10, 100),  
        'wind_speed': round(random.uniform(0, 15), 2) 
    }
# Cities
cities = ["Winnipeg", "Vancouver"]

# Send data to kafka topic
while True:
    for city in cities:
        weather_data = generate_random_weather_data(city)
        producer.send('weather_data_topic', value=weather_data)
        print(f"Sent data: {weather_data}")
    time.sleep(5)
