import json
import os

# Carica la configurazione
def load_config(path="configuration.json"):
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config
