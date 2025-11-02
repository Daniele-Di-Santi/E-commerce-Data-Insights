# extractor.py
import requests
import json
from datetime import datetime
import os
from kafka import KafkaProducer
from loguru import logger


API_URL = "https://fakestoreapi.com/products"
OUTPUT_DIR = "data"

producer = KafkaProducer(bootstrap_servers="kafka:9092",
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def extract_data():
    data = get_data()
    save_data(data)
    send_to_kafka(data)
    return data

def get_data():
    logger.info("Starting data download...")
    
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        return data

    except Exception as e:
        logger.exception(f"Error during the download: {e}")

def save_data(data):
    logger.info("Saving data...")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_path = f"{OUTPUT_DIR}/products_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            import json
            json.dump(data, f, indent=2)

        logger.success(f"Data saved in {file_path}")

    except Exception as e:
        logger.exception(f"Error during data extraction: {e}")

def send_to_kafka(data):
    logger.info("Sending data to Kafka...")

    try:
        for item in data:
            producer.send("raw_products", value=item)
        producer.flush()
        logger.success(f"Data sent to Kafka")

    
    except Exception as e:
        logger.exception(f"Error during sending the data to Kafka: {e}")


if __name__ == "__main__":
    extract_data()



    
