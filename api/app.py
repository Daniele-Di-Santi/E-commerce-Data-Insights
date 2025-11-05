from flask import Flask, send_file, jsonify
from utils.utils import load_config, produce_csv
from utils.mongo.mongo_utils import get_mongodb_connection, insert_documents, read_documents, bulk_upsert
from loguru import logger

app = Flask(__name__)

@app.route('/hello', methods=['GET'])
def hello():
    return jsonify({"message": "hello"})

@app.route('/get_csv', methods=['GET'])
def get_csv_api():
    try:
        get_csv()
        return send_file("../products_data.csv", mimetype="text/csv", as_attachment=True)
    except Exception as e:
        logger.error(f"Errore durante la generazione del CSV: {e}")
        return jsonify({"error": str(e)}), 500
    
def get_csv():
    config = load_config()
    mongo_config = config['mongo']
    output_file = "products_data.csv"
    mongo_db = get_mongodb_connection()
    collection = mongo_db[mongo_config['collection']]
    data = read_documents(collection)
    produce_csv(data, output_file)
    mongo_db.client.close()

if __name__ == "__main__":
    app.run(host="localhost", port=5000)