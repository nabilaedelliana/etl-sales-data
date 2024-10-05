import logging
import os
import schedule
import time

# Ensure dlogs directory is exist
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

# Scheduler to run ETL everyday
schedule.every().day.at("08:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
