#import logging
#import os
#import schedule
#import time

# Pastikan direktori logs sudah ada
#if not os.path.exists('logs'):
#    os.makedirs('logs')

#logging.basicConfig(filename='logs/scheduler.log', level=logging.INFO)

#def job():
#    logging.info("Starting ETL job...")
#    os.system('python scripts/ETL.py')

# Scheduler untuk menjalankan ETL setiap hari
#schedule.every().day.at("22:43").do(job)

#while True:
#    schedule.run_pending()
#    time.sleep(1)


import logging
import os
import schedule
import time

# Pastikan direktori logs sudah ada
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configure logging to console and file
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler('logs/scheduler.log'),
    logging.StreamHandler()  # Add this line
])

def job():
    try:
        logging.info("Starting ETL job...")
        os.system('python etlapp/scripts/ETL.py')
    except Exception as e:
        logging.error(f"Error running ETL job: {e}")

# Scheduler untuk menjalankan ETL setiap hari
schedule.every().day.at("02:21").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
