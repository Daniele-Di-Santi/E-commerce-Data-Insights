import csv
from utils.kafka.kafka_utils import send_to_kafka, get_messages_from_consumer
from utils.utils import load_config
from utils.mongo.mongo_utils import get_mongodb_connection, insert_documents, read_documents, bulk_upsert
from loguru import logger

def get_csv():
    config = load_config()
    mongo_config = config['mongo']
    
    # Initialize MongoDB connection
    mongo_db = get_mongodb_connection()
    collection = mongo_db[mongo_config['collection']]
    data = read_documents(collection)


    
    mongo_db.client.close()