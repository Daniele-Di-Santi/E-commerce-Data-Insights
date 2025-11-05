import json
from utils.kafka.kafka_utils import send_to_kafka, force_clean_kafka_topic, get_messages_from_consumer
from utils.utils import load_config
from loguru import logger
import pandas as pd


def fetch_and_transform():
    config = load_config()

    for topic in config["data_sources"]:
        data = get_data(topic)
        force_clean_kafka_topic(topic)
        data = adjust_id(data)
        send_to_kafka(topic + "_transformed", data)

def transform_to_dataframe(data):
    logger.info("Starting data transformation...")
    
    try:
        dataframe = pd.DataFrame(data)
        logger.success("Data transformation completed")
        return dataframe

    except Exception as e:
        logger.exception(f"Error during data transformation: {e}")

def get_data(topic):
    logger.info(f"Extracting data from Kafka topic: {topic}")

    try:
        data = get_messages_from_consumer(topic=topic)
        logger.success(f"Extracted {len(data)} records from topic: {topic}")
        return data

    except Exception as e:
        logger.exception(f"Error during data extraction from Kafka: {e}")
        return []
    
    
def adjust_id(data):
    for record in data:
        if 'id' in record:
            record['_id'] = str(record['id'])
            del record['id']
    return data

def adjust_rating(data):
    for record in data:
        if 'rating' in record:
            record['rating'] = float(record['rating'])
    return data

if __name__ == "__main__":
    fetch_and_transform()
 
   