# import required libraries
import pandas as pd
import glob
import os
import re
from datetime import datetime, timedelta
import logging
import json

# setting up logging
def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'etl.log')
    logging.basicConfig(filename=log_file,
                        filemode='a',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    
# functions for data source extraction

# Sales data extraction
def extract_sales(parquet_path, csv_path, start_date=None, end_date=None):
    # Read parquet files
    sales_parquet_files = glob.glob(os.path.join(parquet_path), recursive=True)
    sales_parquet_df_list = []

    # Regex to extract year, month, dan day
    year_pattern = re.compile(r'Year=(\d+)')
    month_pattern = re.compile(r'Month=(\d+)')
    day_pattern = re.compile(r'Day=(\d+)')

    # Convert start_date and end_date to datetime objects (for backdating) 
    if start_date:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    logging.info(f"Found {len(sales_parquet_files)} parquet files")
    for file in sales_parquet_files:
        try:
            # extract year, month, dan day from path
            year_match = year_pattern.search(file)
            month_match = month_pattern.search(file)
            day_match = day_pattern.search(file)

            if year_match and month_match and day_match:
                year = year_match.group(1)    # Extracting Year
                month = f"{int(month_match.group(1)):02d}"  # Extracting Month and format to two digits
                day = f"{int(day_match.group(1)):02d}"      # Extracting Day and format to two digits

                # Create a date object
                file_date = datetime(int(year), int(month), int(day))

                # Check if the file_date is in the specified range
                if (start_date and file_date < start_date) or (end_date and file_date > end_date):
                    continue  # Skip this file if the date is out of range
            else:
                logging.warning(f"Could not find Year, Month or Day in: {file}")
                continue  # Skip this file if it is not found

            # read parquet file
            df = pd.read_parquet(file)

            # adding Year, Month, and Day columns
            df['Year'] = year
            df['Month'] = month
            df['Day'] = day
        
            sales_parquet_df_list.append(df)
            logging.info(f"Loaded {len(df)} records from {file}")

        except Exception as e:
            logging.error(f"An error occurred while processing {file}: {e}")
            continue  # Skip this file and continue with the next one

    # merge all parquet dfs
    if sales_parquet_df_list:  # make sure list tis not empty
        sales_parquet_df = pd.concat(sales_parquet_df_list, ignore_index=True)
    else:
        sales_parquet_df = pd.DataFrame()  
    
    # Read csv files
    sales_csv_files = glob.glob(os.path.join(csv_path), recursive=True)
    sales_csv_df_list = []
    # Regex to extract year, month, and day from file name
    csv_file_pattern = re.compile(r'sales_data_(\d{4})-(\d{2})_(\d{2})')
    
    logging.info(f"Found {len(sales_csv_files)} CSV files")
    for file in sales_csv_files:
        logging.info(f"Processing file: {file}")  # print path of file being processed

        try:
            # extract year, month, and day from file name
            file_name = os.path.basename(file)
            match = csv_file_pattern.search(file_name)

            if match:
                year = match.group(1)   # Extracting Year
                month = match.group(2)  # Extracting Month (already two digits)
                day = match.group(3)    # Extracting Day (already two digits)
            else:
                logging.warning(f"Could not find Year, Month or Day in: {file_name}")
                continue  # Skip file ini jika tidak ditemukan

            # read CSV file
            df = pd.read_csv(file)

            # adding Year, Month, and Day columns
            df['Year'] = year
            df['Month'] = month
            df['Day'] = day
            
            sales_csv_df_list.append(df)
            logging.info(f"Loaded {len(df)} records from {file}")
        
        except Exception as e:
            logging.error(f"An error occurred while processing {file}: {e}")
            continue  # Skip this file and continue with the next one

    # merge all csv dfs
    if sales_csv_df_list:  # Pastikan list tidak kosong
        sales_csv_df = pd.concat(sales_csv_df_list, ignore_index=True)
    else:
        sales_csv_df = pd.DataFrame()  # Atau bisa ditangani sesuai kebutuhan

    # If there is data, print the first few lines to make sure everything is good
    if not sales_csv_df.empty:
        logging.info(f"CSV DataFrame head: {sales_csv_df.head()}")
    else:
        logging.warning("No data found in CSV files.")
    
    # merge parquet df and csv df
    if not sales_parquet_df.empty and not sales_csv_df.empty:
        final_sales_df = pd.concat([sales_parquet_df, sales_csv_df], ignore_index=True)
    elif not sales_parquet_df.empty:
        final_sales_df = sales_parquet_df
    elif not sales_csv_df.empty:
        final_sales_df = sales_csv_df
    else:
        final_sales_df = pd.DataFrame()  # Atau bisa ditangani sesuai kebutuhan

    return final_sales_df


# store data extraction
def extract_store(store_path):
    # path to file Store
    storefile = glob.glob(store_path)
    
    # logging the number of files found
    logging.info(f"Found {len(storefile)} store files")

    if not storefile:
        logging.error("No store files found.")
        return pd.DataFrame()  # Return an empty DataFrame if no files found

    # read and merge all store files
    try:
        store_df = pd.concat((pd.read_excel(file) for file in storefile), ignore_index=True)
        logging.info(f"Successfully loaded {len(store_df)} records from store files")
    except Exception as e:
        logging.error(f"An error occurred while concatenating store files: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

    return store_df

# cashier data extraction
def extract_cashier(cashier_path):
    logging.info(f"Extracting cashier data from path: {cashier_path}")
    # Path to Cashier file
    cashierfile = glob.glob(cashier_path)
    # Logging the files found
    logging.info(f"Files found: {cashierfile}")

    # read and merge all cashier files
    cashier_data = []
    for file in cashierfile:
        with open(file) as f:
            data = json.load(f)
            for entry in data.values():
                cashier_data.append(entry)
    cashier_df = pd.DataFrame(cashier_data)
    
    # Check if 'CashierId' column exists
    if 'CashierId' not in cashier_df.columns:
        raise KeyError("Extracted cashier DataFrame is missing 'CashierId' column")
    
    logging.info(f"Extracted cashier DataFrame columns: {cashier_df.columns.tolist()}")
    logging.info(f"First few rows of cashier DataFrame:\n{cashier_df.head()}")
    
    return cashier_df

# cashier data transformation
def transform_cashier(cashier_df):    
    # fill in the missing data in the Name and Email columns with the default values ​​or based on CashierId
    cashier_df['Name'] = cashier_df.groupby('CashierId')['Name'].transform(lambda x: x.ffill().bfill())
    cashier_df['Email'] = cashier_df.groupby('CashierId')['Email'].transform(lambda x: x.ffill().bfill())
    
    # remove duplicates based on CashierId, retaining first entry
    cashier_df = cashier_df.drop_duplicates(subset='CashierId', keep='first')
    
    logging.info(f"Transformed cashier DataFrame columns: {cashier_df.columns.tolist()}")
    logging.info(f"First few rows of transformed cashier DataFrame:\n{cashier_df.head()}")
    
    return cashier_df


# source data transformation
def transform_merge_table(sales_df, store_df, cashier_df):
    # logging to check columns in each dataframe
    logging.info(f"Sales DataFrame columns: {sales_df.columns.tolist()}")
    logging.info(f"Store DataFrame columns: {store_df.columns.tolist()}")
    logging.info(f"Cashier DataFrame columns: {cashier_df.columns.tolist()}")

    # ensure that the 'StoreID' column exists in both sales_df and store_df
    if 'StoreID' not in sales_df.columns:
        logging.error("Sales DataFrame is missing 'StoreID' column")
        raise KeyError("Sales DataFrame is missing 'StoreID' column")
    if 'StoreID' not in store_df.columns:
        logging.error("Store DataFrame is missing 'StoreID' column")
        raise KeyError("Store DataFrame is missing 'StoreID' column")

    # Ensure that the 'CashierId' column exists in both sales_df and cashier_df
    if 'CashierId' not in sales_df.columns:
        logging.error("Sales DataFrame is missing 'CashierId' column")
        raise KeyError("Sales DataFrame is missing 'CashierId' column")
    if 'CashierId' not in cashier_df.columns:
        logging.error("Cashier DataFrame is missing 'CashierId' column")
        raise KeyError("Cashier DataFrame is missing 'CashierId' column")

    # join sales table with store and cashier
    merged_df = sales_df.merge(store_df, on='StoreID').merge(cashier_df, on='CashierId')
    return merged_df

# datamarts transformation
def datamarts(merged_df):
    sales_by_store = merged_df.groupby(['StoreID','StoreName'])['Amount'].sum().reset_index()
    sales_by_store.columns = ['StoreID','StoreName', 'TotalSales']
    sales_by_store['PercentSales'] = sales_by_store['TotalSales']/sum(sales_by_store['TotalSales'])*100

    sales_by_cashier = merged_df.groupby(['CashierId','Name'])['Amount'].sum().reset_index()
    sales_by_cashier.columns = ['CashierId','Name', 'TotalSales']
    sales_by_cashier['PercentSales'] = sales_by_cashier['TotalSales']/sum(sales_by_cashier['TotalSales'])*100

    sales_by_month = merged_df.groupby(['Year','Month'])['Amount'].sum().reset_index()
    sales_by_month.columns = ['Year','Month', 'TotalSales']

    sales_by_day = merged_df.groupby(['Year','Month','Day'])['Amount'].sum().reset_index()
    sales_by_day.columns = ['Year','Month','Day', 'TotalSales']

    sales_by_store_cashier = merged_df.groupby(['StoreID','StoreName','CashierId','Name'])['Amount'].sum().reset_index()
    sales_by_store_cashier.columns = ['StoreID','StoreName','CashierId','Name', 'TotalSales']
    
    return sales_by_store, sales_by_cashier, sales_by_month, sales_by_day, sales_by_store_cashier

# data load
def load_datamarts(sales_by_store, sales_by_cashier, sales_by_month, sales_by_day, sales_by_store_cashier, output_path):
    os.makedirs(output_path, exist_ok=True)
    # Save the datamarts as CSV files
    sales_by_store.to_csv(os.path.join(output_path, 'sales_by_store.csv'), index=False)
    sales_by_cashier.to_csv(os.path.join(output_path, 'sales_by_cashier.csv'), index=False)
    sales_by_month.to_csv(os.path.join(output_path, 'sales_by_month.csv'), index=False)
    sales_by_day.to_csv(os.path.join(output_path, 'sales_by_day.csv'), index=False)
    sales_by_store_cashier.to_csv(os.path.join(output_path, 'sales_by_store_cashier.csv'), index=False)
    logging.info(f"Data saved to {output_path}")

# ETL pipeline
def run_etl_pipeline(start_date=None, end_date=None):
    # use environment variables to get the data directory
    data_dir = os.getenv('DATA_DIR', '/data')
    log_dir = os.getenv('LOG_DIR', '/logs')
    
    setup_logging(log_dir)

    # paths to sales, store, and cashier files
    parquet_path = os.path.join(data_dir, 'sales_data/Year=2024/Month=*/Day=*/*.parquet')
    csv_path = os.path.join(data_dir, 'new_sales_data/*.csv')
    store_path = os.path.join(data_dir, 'master_store.xlsx')
    cashier_path = os.path.join(data_dir, 'cashier_data/*.json')

    # path output
    output_path = os.path.join(data_dir, 'output')

    # Extract
    logging.info('Starting data extraction...')
    sales_df = extract_sales(parquet_path, csv_path, start_date, end_date)
    store_df = extract_store(store_path)
    cashier_df = extract_cashier(cashier_path)
    logging.info('Data extraction completed.')

    # Transform
    logging.info('Starting data transformation...')
    cashier_df = transform_cashier(cashier_df)
    merged_df = transform_merge_table(sales_df, store_df, cashier_df)
    sales_by_store, sales_by_cashier, sales_by_month, sales_by_day, sales_by_store_cashier = datamarts(merged_df)
    logging.info('Data transformation completed.')

    # Load
    logging.info('Starting data loading...')
    load_datamarts(sales_by_store, sales_by_cashier, sales_by_month, sales_by_day, sales_by_store_cashier, output_path)
    logging.info(f"Data loading completed and exist in {output_path}.")

# run ETL pipeline

# parameters for backdating
start_date = '2024-01-01'
end_date = '2024-12-31'

run_etl_pipeline(start_date, end_date)
