# extractor.py
import requests
from utils.kafka.kafka_utils import send_to_kafka
from utils.utils import load_config, save_data, produce_metadata
from loguru import logger

config = load_config()


def extract_data():
    for source in config["data_sources"]:
        data_dir = config['data']["output_dir"] + f"/{source}"
        metadata_dir = config['metadata']["output_dir"] + f"/{source}"
        api_url = config[source]["api_url"]
        data = get_data(api_url)
        metadata = produce_metadata(len(data), source)
        save_data(data, data_dir)
        save_data(metadata, metadata_dir)
        send_to_kafka(source, data)
        send_to_kafka(config['metadata']["topic"]+source, metadata)

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



    
