import json
import os
from datetime import datetime, timezone
from loguru import logger

DIR_MAX_FILES = 3

# Load Configuration
def load_config(path="utils/configuration.json"):
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config

# Save Data to Local Storage
def save_data(data, output_dir):
    while check_dir_size(output_dir, DIR_MAX_FILES):
        clean_dir(output_dir)

    logger.info("Saving data...")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(output_dir, exist_ok=True)
        file_path = f"{output_dir}/products_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            import json
            json.dump(data, f, indent=2)

        logger.success(f"Data saved in {file_path}")

    except Exception as e:
        logger.exception(f"Error during data extraction: {e}")

def check_dir_size(dir_path, max_files):
    try:
        files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
        if len(files) >= max_files:
            return True
        return False
    except Exception as e:
        logger.exception(f"Error while checking directory size: {e}")
        return False

def clean_dir(dir_path):
    logger.info(f"Cleaning {dir_path}...")
    
    try:
        # Check if directory exists
        if not os.path.exists(dir_path):
            logger.warning(f"Directory {dir_path} does not exist")
            return

        # Get list of files with their creation times
        files = []
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            if os.path.isfile(file_path):
                creation_time = os.path.getctime(file_path)
                files.append((file_path, creation_time))

        if not files:
            logger.info(f"No files found in {dir_path}")
            return

        # Find the oldest file
        oldest_file = min(files, key=lambda x: x[1])[0]

        # Remove the oldest file
        os.remove(oldest_file)
        logger.success(f"Removed oldest file: {oldest_file}")

    except Exception as e:
        logger.exception(f"Error while cleaning directory: {e}")

def produce_metadata(data_size, source):
    metadata = {
        "source": source,
        "record_count": data_size,
        "timestamp": str(datetime.now(timezone.utc).isoformat())
    }
    return metadata
