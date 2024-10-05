import logging
import os
import schedule
import time

# Pastikan direktori logs sudah ada
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(filename='logs/scheduler.log', level=logging.INFO)

def job():
    logging.info("Starting ETL job...")
    os.system('python scripts/ETL.py')

# Scheduler untuk menjalankan ETL setiap hari
schedule.every().day.at("22:43").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
