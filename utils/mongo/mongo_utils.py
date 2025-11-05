from typing import List, Dict, Any
from utils.utils import load_config
from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

config = load_config()

# -- MongoDB helpers -------------------------------------------------
def get_mongodb_connection(host: str = config['mongo']['host'], 
                         port: int = config['mongo']['port'], 
                         database: str = config['mongo']['database']) -> Database:
    """
    Create a connection to MongoDB and return the database object
    """
    logger.info(f"Connecting to MongoDB at {host}:{port}, database: {database}")
    client = MongoClient(host, port)
    logger.success("Connected to MongoDB successfully")
    return client[database]

def insert_documents(collection: Collection, 
                    documents: List[Dict[str, Any]]) -> None:
    """
    Insert multiple documents into a MongoDB collection
    """
    logger.info(f"Inserting {len(documents)} documents into collection: {collection.name}")
    try:
        collection.insert_many(documents)
        logger.success(f"Inserted {len(documents)} documents successfully")
    except Exception as e:
        logger.exception(f"Error inserting documents: {e}")

def read_documents(collection: Collection, 
                  query: Dict[str, Any] = None, 
                  projection: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Read documents from a MongoDB collection with optional query and projection
    """
    logger.info(f"Reading documents from collection: {collection.name} with query: {query} and projection: {projection}")
    if query is None:
        query = {}
    
    try:
        cursor = collection.find(query, projection)
        return list(cursor)
    except Exception as e:
        logger.exception(f"Error reading documents: {e}")
        return []

def bulk_upsert(collection: Collection, 
                documents: List[Dict[str, Any]], 
                key_field: str) -> None:
    """
    Perform bulk upsert operations using a specific key field
    """
    if not documents:
        return
        
    operations = [
        {
            'replace_one': {
                'filter': {key_field: doc[key_field]},
                'replacement': doc,
                'upsert': True
            }
        }
        for doc in documents
    ]
    
    collection.bulk_write(operations)