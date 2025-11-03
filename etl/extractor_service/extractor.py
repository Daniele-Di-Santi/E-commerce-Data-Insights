# extractor.py
import requests
import json
from etl.kafka.kafka_utils import init_kafka, send_to_kafka
from etl.utils.utils import load_config, save_data

import os
from loguru import logger

config = load_config()
producer = init_kafka()
OUTPUT_DIR = config["output_dir"]


def extract_data():
    for source in config["data_sources"]:
        api_url = config[source]["api_url"]
        data = get_data(api_url)
        dir = OUTPUT_DIR + f"/{source}"
        save_data(data, dir)
        send_to_kafka(producer, source, data)
        return data

def get_data(api_url):
    logger.info("Starting data download...")
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        return data

    except Exception as e:
        logger.exception(f"Error during the download: {e}")


if __name__ == "__main__":
    extract_data()



    
