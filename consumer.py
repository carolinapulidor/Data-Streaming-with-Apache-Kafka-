import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2

# Configure logging
logging.basicConfig(filename='consumer.log', level=logging.INFO)

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic = 'weather_data_topic'
group_id = 'weather-consumer'

# PostgreSQL database configuration
db_host = 'localhost'
db_name = 'weather_database'
db_user = 'postgres'
db_password = 'root123'

# Helper function to load data into PostgreSQL database
def load_data_to_postgres(weather_data):
    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
        cursor = conn.cursor()
        for item in weather_data:
            cursor.execute("INSERT INTO weather_data (city, temperature, wind_speed, humidity, received_at) VALUES (%s, %s, %s, %s, %s)",
                           (item['city'], item['temperature'], item['wind_speed'], item['humidity'], item['received_at']))
            conn.commit()
            logging.info("Data loaded into PostgreSQL successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.error("Error loading data into PostgreSQL: %s", error)
    finally:
        if conn is not None:
            conn.close()

# Create Kafka consumer
consumer = KafkaConsumer(topic,
                         group_id=group_id,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='latest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Main loop to consume weather data
for message in consumer:
    weather_database = message.value
    received_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    weather_database['received_at'] = received_at
    print("Received weather data:", weather_database)
    logging.info("Received weather data: %s", weather_database)

    # Load data into PostgreSQL database
    load_data_to_postgres([weather_database])

    print("end db:")
