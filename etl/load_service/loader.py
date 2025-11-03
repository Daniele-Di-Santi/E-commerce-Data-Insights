import json
from etl.utils.kafka.kafka_utils import fetch_one_message_from_topic, init_kafka, send_to_kafka, get_messages_from_consumer, init_kafka_consumer
from etl.utils.utils import load_config
from etl.utils.mongo.mongo_utils import get_mongodb_connection, insert_documents, read_documents, bulk_upsert
from loguru import logger


def load_data():
    config = load_config()
    topics = config['data_sources']
    mongo_config = config['mongo']
    
    # Initialize MongoDB connection
    mongo_db = get_mongodb_connection(
        host=mongo_config['host'],
        port=mongo_config['port'],
    )
    collection = mongo_db[mongo_config['collection']]
    
    for topic in topics:
        data = get_data(topic)
        if data:
            # Upsert data into MongoDB
            insert_documents(collection, data)
            logger.info(f"Insert {len(data)} records from topic '{topic}' into MongoDB collection '{mongo_config['collection']}'")
        else:
            logger.warning(f"No data retrieved from topic '{topic}'")



def get_data(topic):
    consumer = init_kafka_consumer(topic)
    logger.info(f"Extracting data from Kafka topic: {topic}")

    try:
        data = get_messages_from_consumer(consumer, timeout_ms=2000)
        logger.success(f"Extracted {len(data)} records from topic: {topic}")
        return data

    except Exception as e:
        logger.exception(f"Error during data extraction from Kafka: {e}")
        return []
    
    finally:
        consumer.close()


if __name__ == "__main__":
    load_data()