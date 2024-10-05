# ETL Project with Scheduler and Docker
## Overview
This project demonstrates an Extract, Transform, Load (ETL) process using Python, scheduled to run periodically using a scheduler script, and containerized with Docker for local execution. The ETL process involves extracting data from multiple sources, transforming it, and loading it into a data mart.

## Folder Structure
.
│   docker-compose.yml
│   dockerfile
│   README.md
│   requirements.txt
│
├───.data
├───data
└───etlapp
    ├───data
    │   │   master_store.xlsx
    │   │
    │   ├───cashier_data
    │   │       .DS_Store
    │   │       cashier_data20240101023456.json
    │   │       cashier_data20240102025110.json
    │   │       cashier_data20240102035221.json
    │   │       cashier_data20240105031331.json
    │   │       cashier_data20240107015214.json
    │   │       cashier_data20240108051442.json
    │   │       cashier_data20240201035144.json
    │   │       cashier_data20240201051442.json
    │   │
    │   ├───new_sales_data
    │   │       sales_data_2024-02_05.csv
    │   │       sales_data_2024-02_06.csv
    │   │       sales_data_2024-02_07.csv
    │   │
    │   ├───output
    │   │       sales_by_cashier.csv
    │   │       sales_by_day.csv
    │   │       sales_by_month.csv
    │   │       sales_by_store.csv
    │   │       sales_by_store_cashier.csv
    │   │
    │   └───sales_data
    │       │   .DS_Store
    │       │
    │       └───Year=2024
    │           │   .DS_Store
    │           │
    │           ├───Month=1
    │           │   │   .DS_Store
    │           │   │
    │           │   ├───Day=27
    │           │   │       9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │           │   │
    │           │   ├───Day=28
    │           │   │       9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │           │   │
    │           │   ├───Day=29
    │           │   │       9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │           │   │
    │           │   ├───Day=30
    │           │   │       9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │           │   │
    │           │   └───Day=31
    │           │           9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │           │
    │           └───Month=2
    │               │   .DS_Store
    │               │
    │               ├───Day=1
    │               │       9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │               │
    │               ├───Day=2
    │               │       9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │               │
    │               ├───Day=3
    │               │       9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │               │
    │               └───Day=4
    │                       9bd979fed625436f93f12c3a34c3e2c4-0.parquet
    │
    ├───logs
    │       etl.log
    │
    └───scripts
            ETL.py
            scheduler.py

## Prerequisites
Docker
Docker Compose

## Setup
### Step 1: Clone the repository in sh / cmd
git clone <repository-url>
cd <repository-directory>
### Step 2: Build the Docker image / cmd
docker build -t etlapp:latest .
### Step 3: Run Docker Compose / cmd
docker-compose up


## ETL Process
### ETL Script (ETL.py)
The ETL script performs the following tasks:

Extraction: Reads data from various sources including CSV, JSON, and Parquet files.
Transformation: Cleanses and transforms the data into a consistent format.
Loading: Loads the transformed data into the data mart.

### Scheduler Script (scheduler.py)
The scheduler script ensures the ETL process runs periodically. It uses the schedule library to set up regular intervals for the ETL job.

### Dockerfile
The Dockerfile sets up the environment for running the ETL process. It installs necessary dependencies and copies the scripts into the Docker image.

### Docker Compose
The docker-compose.yml file defines the services to run the ETL process and the scheduler.

### Logs
Logs are stored in the logs/etl.log file. You can check the logs to monitor the ETL process.

### Data
The data is stored in the etlapp/data directory and consists of various input files and the output data mart.

## Conclusion
This ETL project demonstrates a complete pipeline for extracting, transforming, and loading data using Python, scheduled with a scheduler script, and containerized using Docker. Follow the setup steps to get the ETL process running on your local machine.

