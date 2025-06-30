from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, concat_ws, date_format, regexp_replace, when, lpad, lit, dayofweek, split, lag, unix_timestamp,abs, sum as spark_sum
from dotenv import load_dotenv, find_dotenv
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from bs4 import BeautifulSoup
from threading import Thread, Lock
import os, requests, time
from pyspark.sql import Window

load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
EXCHANGE_RATE_API_URL = os.getenv('EXCHANGE_RATE_API_URL')
HDFS_CHECKPOINT = os.getenv('HDFS_CHECKPOINT')
HDFS_PATH = os.getenv('HDFS_PATH')

exchange_rate_lock = Lock()
current_exchange_rate = None

def create_spark_session(app_name):
    """
    Create and return a Spark session
    
    Args:
        app_name (str): The name of the Spark application
    
    Returns:
        SparkSession: An instance of SparkSession

    Raises:
        Exception: If the Spark session cannot be created
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        print(f"Spark session created with app name: {app_name}")
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    except Exception as e:
        raise Exception(f"Failed to create Spark session: {e}")
    

def read_stream_from_kafka(spark, topic, bootstrap_servers):
    """
    Read a stream of data from a Kafka topic using Spark

    Args:
        spark (SparkSession): The Spark session instance
        topic (str): The Kafka topic to read from
        bootstrap_servers (str): The Kafka bootstrap servers to connect to

    Returns:
        DataFrame: A Spark DataFrame containing the streamed data

    Raises:
        Exception: If the stream cannot be read from Kafka
    """

    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
        print(f"Reading stream from Kafka topic: {topic}")
        return df
    except Exception as e:
        raise Exception(f"Failed to read stream from Kafka: {e}")
    
def parse_kafka_stream(df, schema):
    """
    Parse the Kafka stream DataFrame to extract the value into a schema and return a new DataFrame

    Args:
        df (DataFrame): The DataFrame containing the Kafka stream data

    Returns:
        DataFrame: A DataFrame with the parsed values

    Raises:
        Exception: If the parsing fails
    """
    try:
        parsed_df = df.select(col("value").cast("string").alias("value")) \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        print("Parsed Kafka stream data")
        return parsed_df
    except Exception as e:
        raise Exception(f"Failed to parse Kafka stream data: {e}")
    
def define_schema():
    """
    Define the schema for the Kafka stream data

    Returns:
        StructType: The schema for the Kafka stream data
    """

    return StructType([
        StructField("User", StringType()),
        StructField("Card", StringType()),
        StructField("Year", StringType()),
        StructField("Month", StringType()),
        StructField("Day", StringType()),
        StructField("Time", StringType()),
        StructField("Amount", StringType()),
        StructField("Use Chip", StringType()),
        StructField("Merchant Name", StringType()),
        StructField("Merchant City", StringType()),
        StructField("Merchant State", StringType()),
        StructField("Zip", StringType()),
        StructField("MCC", StringType()),
        StructField("Errors?", StringType()),
        StructField("Is Fraud?", StringType()),
    ])

def get_exchange_rate(url, default_rate=26000.0):
    """
    Fetch the exchange rate from a given URL.

    Args:
        url (str): The URL to fetch the exchange rate from

    Returns:
        float: The exchange rate value

    Prints error:
        ValueError: If the exchange rate element is not found in the HTML
        Exception: If there is an error fetching the exchange rate
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        rate_element = soup.find('span', class_='text-success')
        if rate_element:
            rate_text = rate_element.text.strip()
            rate_value = float(rate_text.replace(',', '').replace('.', ''))
            return rate_value
        else:
            print("Warning: Exchange rate element not found. Return default rate")
            return default_rate
    except requests.RequestException as e:
        print(f"Failed to fetch exchange rate: {e}. Return default rate")
        return default_rate
    
def update_exchange_rate(url, interval_seconds=60):
    """
    Update the exchange rate periodically from a given URL.

    Args:
        url (str): The URL to fetch the exchange rate from
        interval_seconds (int): The interval in seconds to update the exchange rate

    Raises:
        Exception: If there is an error fetching the exchange rate
    """
    global current_exchange_rate
    while True:
        try:
            new_rate = get_exchange_rate(url)
            with exchange_rate_lock:
                current_exchange_rate = new_rate
            print(f"Updated exchange rate: {current_exchange_rate}")

        except Exception as e:
            print(f"Error updating exchange rate: {e}")
        time.sleep(interval_seconds)

def get_current_exchange_rate():
    """
    Get the current exchange rate, ensuring thread safety

    Returns:
        float: The current exchange rate
    """
    with exchange_rate_lock:
        return current_exchange_rate
    
def transform_data(df):
    """
    Transform the DataFrame

    Args:
        df (DataFrame): The DataFrame to transform

    Returns:
        DataFrame: The transformed DataFrame

    Raises:
        Exception: If the transformation fails
    """
    try:
        # Get the current exchange rate
        exchange_rate = get_current_exchange_rate()
        if exchange_rate is None:
            raise Exception("Exchange rate is not available.")

        # Add timestamp column by combining Year, Month, Day, and Time
        df = df.withColumn(
            "Timestamp",
            to_timestamp(concat_ws(" ", concat_ws("-", lpad(col("Day"), 2, "0"), lpad(col("Month"), 2, "0"), col("Year")), col("Time")), "dd-MM-yyyy HH:mm")
        ).withColumn(
            "Date",
            date_format(col("Timestamp"), "dd-MM-yyyy") # Add additional columns for date components
        ).withColumn(
            "Amount_USD",
            regexp_replace(col("Amount"), "[$,]", "").cast(DoubleType()) # Add Amount_USD and Amount_VND columns
        ).withColumn(
            "Amount_VND",
            (abs(col("Amount_USD") * lit(exchange_rate))).cast(DoubleType())
        ).withColumn(
            "Transaction Type",
            when (col("Amount_USD") > 0, "Credit")
            .when (col("Amount_USD") < 0, "Debit")
            .otherwise("Unknown") # Add Transaction Type based on Amount_USD is greater than, less than, or equal to zero
        ).withColumn(
            "Hour",
            split(col("Time"), ":").getItem(0).cast("int")
        ).withColumn(
            "Is Weekend?",
            when(dayofweek(col("Timestamp")).isin([1, 7]), "Yes").otherwise("No")
        )

        return df.select(
            "Timestamp",
            "User",
            "Date",
            "Year",
            "Month",
            "Day",
            "Time",
            "Hour",
            "Amount_VND",
            "Merchant Name",
            "Merchant City",
            "Is Fraud?",
            "Transaction Type",
            "Is Weekend?"
        )
    except Exception as e:
        raise Exception(f"Failed to transform data: {e}")
    
def process_batch(batch_df, epoch_id):
    from pyspark.sql import Window
    from pyspark.sql.functions import lag, unix_timestamp, sum as spark_sum, row_number, col

    if batch_df.count() == 0:
        return

    window_user = Window.partitionBy("User").orderBy("Timestamp")
    prev_timestamp_col = lag("Timestamp").over(window_user)
    time_diff_col = (unix_timestamp(col("Timestamp")) - unix_timestamp(prev_timestamp_col))
    within_threshold_col = (time_diff_col <= 300)
    reset_col = (~within_threshold_col) | prev_timestamp_col.isNull()
    batch_df = batch_df.withColumn("_reset_flag", reset_col.cast("int"))
    batch_df = batch_df.withColumn("_group_id", spark_sum("_reset_flag").over(window_user))
    group_window = Window.partitionBy("User", "_group_id").orderBy("Timestamp")
    batch_df = batch_df.withColumn("Consecutive Count", row_number().over(group_window) - 1)
    batch_df = batch_df.drop("_reset_flag", "_group_id")

    # Write to HDFS
    batch_df.write.mode("append").option("header", "true").csv(HDFS_PATH)
    # Write to console for debugging
    batch_df.show(truncate=False)

def stop_spark_session(spark):
    """
    Stop the Spark session gracefully

    Args:
        spark (SparkSession): The Spark session instance to stop

    Raises: 
        Exception: If the Spark session cannot be stopped
    """
    try:
        print("Stopping Spark session...")
        if spark:
            spark.stop()
            print("Spark session stopped successfully.")
        else:
            print("Spark session is already stopped or was never started.")
    except Exception as e:
        raise Exception(f"Failed to stop Spark session: {e}")

def main():
    try:
        # Load environment variables
        print("Loading environment variables...")
        if not KAFKA_BOOTSTRAP_SERVERS or not KAFKA_TOPIC or not EXCHANGE_RATE_API_URL or not HDFS_CHECKPOINT or not HDFS_PATH:
            raise ValueError("Missing required environment variables. Please check your .env file.")
        
        # Create Spark session
        print("Creating Spark session...")
        spark = create_spark_session('KafkaStreamProcessor')

        # Define schema for the Kafka stream data
        schema = define_schema()

        # Read stream from Kafka and parse it
        print("Reading stream from Kafka...")

        kafka_df = read_stream_from_kafka(spark, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
        parsed_df = parse_kafka_stream(kafka_df, schema)
        print("Kafka stream read and parsed successfully.")

        # Initialize the exchange rate
        global current_exchange_rate
        current_exchange_rate = get_exchange_rate(EXCHANGE_RATE_API_URL)
        print(f"Initial exchange rate: {current_exchange_rate}")

        print("Initalizing exchange rate updater thread...")
        # Start a thread to update the exchange rate periodically
        exchange_rate_thread = Thread(target=update_exchange_rate, args=(EXCHANGE_RATE_API_URL, 60), daemon=True)
        exchange_rate_thread.start()

        # Transform the parsed DataFrame (không tính consecutive_count ở đây)
        print("Transforming data...")
        transformed_df = transform_data(parsed_df)
        print("Data transformation complete.")

        # Use foreachBatch to calculate consecutive_count and write to HDFS
        print("Writing transformed data to HDFS and console using foreachBatch...")
        query = transformed_df.writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", HDFS_CHECKPOINT) \
            .start()
        query.awaitTermination()

    except ValueError as ve:
        print(f"Configuration error: {ve}")

    except KeyboardInterrupt:
        print("\nStreaming stopped by user (CTRL+C)")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Stop the Spark session gracefully
        if 'spark' in locals():
            stop_spark_session(spark)

if __name__ == "__main__":
    main()







