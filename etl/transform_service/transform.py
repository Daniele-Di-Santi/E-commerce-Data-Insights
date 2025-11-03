import json
from etl.kafka.kafka_utils import fetch_one_message_from_topic, init_kafka, send_to_kafka, get_messages_from_consumer, init_kafka_consumer
from etl.utils.utils import load_config
from loguru import logger
import pandas as pd

config = load_config()

def fetch_and_transform():
    for topic in config["data_sources"]:
        data = get_data(topic)
        dataframe = transform_to_dataframe(data)
        print(dataframe.head())

        producer = init_kafka()
        send_to_kafka(producer, topic + "_transformed", dataframe.to_dict(orient="records"))

        return dataframe

def transform_to_dataframe(data):
    logger.info("Starting data transformation...")
    
    try:
        dataframe = pd.DataFrame(data)
        logger.success("Data transformation completed")
        return dataframe

    except Exception as e:
        logger.exception(f"Error during data transformation: {e}")

def get_data(topic):
    consumer = init_kafka_consumer(topic)
    logger.info(f"Extracting data from Kafka topic: {topic}")

    try:
        data = get_messages_from_consumer(consumer, timeout_ms=2000, max_messages=5)
        logger.success(f"Extracted {len(data)} records from topic: {topic}")
        return data

    except Exception as e:
        logger.exception(f"Error during data extraction from Kafka: {e}")
        return []
    
    finally:
        consumer.close()
    


if __name__ == "__main__":
    fetch_and_transform()
   