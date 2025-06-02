from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, to_date, regexp_replace, lit, date_format, to_timestamp, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests, os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
EXCHANGE_RATE_API_URL = os.getenv("EXCHANGE_RATE_API_URL")
HDFS_PATH = os.getenv("HDFS_PATH")
HDFS_CHECKPOINT = os.getenv("HDFS_CHECKPOINT")

def get_exchange_rate():
    try:
        response = requests.get(EXCHANGE_RATE_API_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        rate_text = soup.find('span', {'class': 'text-success'})
        
        if rate_text:
            value = rate_text.text.strip().replace('.', '').replace(',', '')
            return float(value)
        else:
            print("Không tìm thấy tỷ giá.")
            return 26000.0
    except Exception as e:
        print(f"Lỗi khi lấy tỷ giá: {e}")
        return 26000.0

def create_spark_session():
    spark = SparkSession.builder.appName("StreamingJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def define_schema():
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

def read_from_kafka(spark, schema):
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")

    return df_parsed

def transform_data(df, exchange_rate):
    df = df.withColumn(
        "Timestamp",
        date_format(to_timestamp(concat_ws(" ", concat_ws("-", col("Year"), col("Month"), col("Day")), col("Time")),
                     "yyyy-M-d HH:mm"), "dd-MM-yyyy HH:mm")
    )
    df = df.withColumn(
        "Date",
        date_format(to_date(concat_ws("-", col("Year"), col("Month"), col("Day")), "yyyy-M-d"), "dd-MM-yyyy")
    )
    df = df.withColumn(
        "Hour",
        split(col("Time"), ":").getItem(0).cast("int")
    )
    df = df.withColumn(
        "Amount_Clean", 
        regexp_replace("Amount", "[$,]", "").cast(DoubleType())
    )
    df = df.withColumn(
        "Amount_VND", 
        (col("Amount_Clean") * lit(exchange_rate)).cast("int")
    )
    columns_to_keep = [
        "User", "Card", "Year", "Month", "Day", "Time",
        "Use Chip", "Merchant Name", "Merchant City",
        "Is Fraud?", "Date", "Hour", "Amount_VND", "Timestamp"
    ]

    return df.select(*columns_to_keep)

def write_output(df):
    query_console = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query_hdfs = df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", HDFS_PATH) \
        .option("checkpointLocation", HDFS_CHECKPOINT) \
        .option("header", True) \
        .start()

    query_console.awaitTermination()
    query_hdfs.awaitTermination()

def main():
    exchange_rate = get_exchange_rate()
    spark = create_spark_session()
    schema = define_schema()
    
    df_kafka = read_from_kafka(spark, schema)
    df_transformed = transform_data(df_kafka, exchange_rate)
    
    write_output(df_transformed)

if __name__ == "__main__":
    main()