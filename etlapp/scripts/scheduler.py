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

# Make sure the logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(filename='logs/scheduler.log', level=logging.INFO)

def job():
    logging.info("Starting ETL job...")
    try:
        # Define the path to the ETL script
        etl_script_path = os.path.join('etlapp', 'scripts', 'ETL.py')
        # Run the ETL script
        os.system(f'python {etl_script_path}')
        logging.info("ETL job completed successfully.")
    except Exception as e:
        logging.error(f"ETL job failed: {e}")

# Schedule the ETL to run every day at a specified time
schedule.every().day.at("00:15").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
