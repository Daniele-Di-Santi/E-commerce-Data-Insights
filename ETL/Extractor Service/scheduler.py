# scheduler.py
from apscheduler.schedulers.blocking import BlockingScheduler
from extractor import extract_data

scheduler = BlockingScheduler()

# Esegui ogni 6 ore
scheduler.add_job(extract_data, "interval", hours=6)

print("Scheduler started... (every 6 hours)")
scheduler.start()
