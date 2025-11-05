import json
from utils.kafka.kafka_utils import force_clean_kafka_topic, get_messages_from_consumer
from utils.utils import load_config
from utils.mongo.mongo_utils import get_mongodb_connection, insert_documents, read_documents, bulk_upsert
from loguru import logger


def load_data():
    config = load_config()
    topics = config['data_sources']
    mongo_config = config['mongo']
    
    # Initialize MongoDB connection
    mongo_db = get_mongodb_connection()
    collection = mongo_db[mongo_config['collection']]
    
    for topic in topics:
        topic = topic+ "_transformed"
        data = get_data(topic)
        force_clean_kafka_topic(topic)
        if data:
            # Upsert data into MongoDB
            insert_documents(collection, data)
            logger.info(f"Insert {len(data)} records from topic '{topic}' into MongoDB collection '{mongo_config['collection']}'")
        else:
            logger.warning(f"No data retrieved from topic '{topic}'")
    
    mongo_db.client.close()



def get_data(topic):
    logger.info(f"Extracting data from Kafka topic: {topic}")

    try:
        data = get_messages_from_consumer(topic=topic)
        logger.success(f"Extracted {len(data)} records from topic: {topic}")
        return data

    except Exception as e:
        logger.exception(f"Error during data extraction from Kafka: {e}")
        return []
    


if __name__ == "__main__":
    load_data()