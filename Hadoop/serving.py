from dotenv import load_dotenv, find_dotenv
import os, re, datetime, requests
from hdfs import InsecureClient
from pyspark.sql.functions import col, lit
import pandas as pd
from io import StringIO

load_dotenv(find_dotenv())

def read_last_push_timestamp(file_path):
    """
    Reads the last  push timestamp from a file.

    Args:
        file_path (str): The path to the file containing the last push timestamp.

    Returns:
        timestamp (datetime): The last push timestamp as datetime string, or None if the file does not exist or is empty.

    Raises:
        ValueError: If the timestamp format in the file is invalid.
        Exception: If there is an error reading the file.
    """

    if not os.path.exists(file_path):
        return None
    
    try:
        with open(file_path, 'r') as file:
            timestamp_str = file.read().strip()

        if not timestamp_str:
            return None
        
        return datetime.datetime.strptime(timestamp_str, '%d-%m-%Y %H:%M')
    
    except ValueError:
        raise ValueError(f"Invalid timestamp format in file {file_path}. Expected format: dd-MM-yyyy HH:MM")

    except Exception as e:
        raise Exception(f"Error reading the file {file_path}: {e}")
    
def create_hdfs_client(hdfs_namenode_path, hdfs_username):
    """
    Create and return an HDFS client instance.

    Args:
        hdfs_namenode_path (str): The HDFS namenode path to connect to.
        hdfs_username (str): The username for HDFS authentication.

    Returns:
        InsecureClient: An instance of InsecureClient connected to the specified HDFS namenode. 
    
    Raises:
        Exception: If the client cannot be created or connected.
    """
    try:
        client = InsecureClient(hdfs_namenode_path, user=hdfs_username)
        print(f"HDFS client created and connected to {hdfs_namenode_path} as {hdfs_username}")
        return client
    except Exception as e:
        raise Exception(f"Failed to create HDFS client: {e}")

def read_latest_data_from_hdfs(hdfs_namenode_path, hdfs_username, hdfs_data_path, last_push_timestamp):
    """
    Read the latest data from HDFS based on the last push timestamp.

    Args:
        hdfs_namenode_path (str): The HDFS namenode path to connect to.
        hdfs_username (str): The username for HDFS authentication.
        hdfs_data_path (str): The path in HDFS where the data is stored.
        last_push_timestamp (str): The last push timestamp to filter the data.

    Returns:
        DataFrame: A DataFrame containing the latest data from HDFS that has a timestamp greater than the last push timestamp.

    Raises:
        FileNotFoundError: If the specified HDFS data path does not exist or no CSV files are found.
        Exception: If there is an error reading from HDFS.
    """
    try:
        print(f"Connecting to HDFS at {hdfs_namenode_path} as {hdfs_username}...")
        client = create_hdfs_client(hdfs_namenode_path, hdfs_username)
        if not client.status(hdfs_data_path, strict=False):
            raise FileNotFoundError(f"The specified HDFS data path {hdfs_data_path} does not exist.")
        
        files = client.list(hdfs_data_path)
        csv_files = [f for f in files if (f.endswith('.csv') and re.match(r'part-.*', f))]

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in the specified HDFS data path {hdfs_data_path}.")
        
        latest_data = pd.DataFrame()

        for file in csv_files:
            file_path = f"{hdfs_data_path.rstrip('/')}/{file}"

            with client.read(file_path, encoding='utf-8') as reader:
                csv_data = reader.read()

            if not csv_data.strip():
                continue

            df = pd.read_csv(StringIO(csv_data), parse_dates=['Timestamp'], date_format='ISO8601')
            if 'Timestamp' in df.columns:
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
                df = df.dropna(subset=['Timestamp'])
            

            if last_push_timestamp is None:
                latest_data = pd.concat([latest_data, df], ignore_index=True)
            else:
                # Convert last_push_timestamp to timezone-aware if needed
                if df['Timestamp'].dt.tz is not None and last_push_timestamp.tzinfo is None:
                    import pytz
                    last_push_timestamp = pytz.timezone('Asia/Ho_Chi_Minh').localize(last_push_timestamp)
                elif df['Timestamp'].dt.tz is None and last_push_timestamp.tzinfo is not None:
                    last_push_timestamp = last_push_timestamp.replace(tzinfo=None)
                
                df = df[df['Timestamp'] > last_push_timestamp]

                latest_data = pd.concat([latest_data, df], ignore_index=True)

        print(f"Total records read from HDFS: {len(latest_data)}")
        return latest_data

    except FileNotFoundError as e:
        raise FileNotFoundError(f"Error reading from HDFS: {e}")
    
    except Exception as e:
        raise Exception(f"Failed to read latest data from HDFS: {e}")
    

def push_data_to_powerbi(df, powerbi_url, batch_size=10000):
    """
    Push data to Power BI in batches.

    Args:
        df (DataFrame): The DataFrame containing the data to be pushed.
        powerbi_url (str): The Power BI API URL to push the data to.
        batch_size (int): The number of records to push in each batch.

    Raises:
        Exception: If there is an error pushing data to Power BI.
    """
    try:
        if df.empty:
            print("No new data to push to Power BI.")
            return

        # Create a copy for Power BI (without Timestamp)
        df_for_powerbi = df.copy()
        if 'Timestamp' in df_for_powerbi.columns:
            df_for_powerbi = df_for_powerbi.drop(columns=['Timestamp'])

        df_for_powerbi = df_for_powerbi.fillna('')
        df_for_powerbi = df_for_powerbi.infer_objects(copy=False)
        records = df_for_powerbi.to_dict(orient='records')
        total = len(records)

        headers = {
            'Content-Type': 'application/json',
        }

        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            payload = {
                "rows": batch
            }

            response = requests.post(powerbi_url, json=payload, headers=headers)

            if response.status_code != 200:
                with open("error_batches.log", "a") as err_log:
                    err_log.write(f"Batch {i // batch_size + 1} failed: {response.text}\n")
                raise Exception(f"Failed to push data to Power BI: {response.text}")

            print(f"Pushed batch {i // batch_size + 1} of {total // batch_size + 1} to Power BI. Total records pushed: {len(batch)}")

    except Exception as e:
        raise Exception(f"Failed to push data to Power BI: {e}")
    
def update_last_push_timestamp(file_path, df):
    """
    Update the last push timestamp in a file.

    Args:
        file_path (str): The path to the file where the last push timestamp will be saved.
        df (DataFrame): The DataFrame containing the data that was pushed, used to determine the last push timestamp.

    Raises:
        Exception: If there is an error writing to the file.
    """
    try:
        if df.empty or 'Timestamp' not in df.columns:
            print("No data to update last push timestamp.")
            return
        
        last_timestamp = df['Timestamp'].max().strftime("%d-%m-%Y %H:%M")

        with open(file_path, 'w') as file:
            file.write(last_timestamp)

        print(f"Last push timestamp updated to {last_timestamp} in file {file_path}.")

    except Exception as e:
        raise Exception(f"Failed to update last push timestamp in file {file_path}: {e}")
    
def main():
    hdfs_namenode_path = os.getenv('HDFS_NAMENODE_URL')
    hdfs_username = os.getenv('HDFS_USERNAME')
    hdfs_data_path = os.getenv('HDFS_DATA_PATH')
    powerbi_url = os.getenv('POWERBI_URL')
    last_push_timestamp_file_path = os.getenv('LAST_PUSH_TIMESTAMP_FILE_PATH')

    if not all([hdfs_namenode_path, hdfs_username, hdfs_data_path, powerbi_url, last_push_timestamp_file_path]):
        raise ValueError("One or more environment variables are not set. Please check your configuration.")

    try:
        print("Starting data push to Power BI...")

        print(f"\nReading last push timestamp from {last_push_timestamp_file_path}...")
        last_push_timestamp = read_last_push_timestamp(last_push_timestamp_file_path)
        print(f"Last push timestamp: {last_push_timestamp}")

        print(f"\nReading latest data from HDFS at {hdfs_data_path}...")
        latest_data = read_latest_data_from_hdfs(hdfs_namenode_path, hdfs_username, hdfs_data_path, last_push_timestamp)

        if not latest_data.empty:
            print("\nPushing latest data to Power BI...")
            push_data_to_powerbi(latest_data, powerbi_url)
            print("Data pushed successfully to Power BI.")

            print("\nUpdating last push timestamp...")
            update_last_push_timestamp(last_push_timestamp_file_path, latest_data)
        else:
            print("No new data to push to Power BI.")
        
    except ValueError as ve:
        print(f"Configuration error: {ve}")

    except FileNotFoundError as fnf_error:
        print(f"File error: {fnf_error}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()