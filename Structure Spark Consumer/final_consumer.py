from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType
from pyspark.sql.functions import split
from datetime import datetime
from pyspark.sql.functions import window
import shutil
import time
import io
import pyarrow as pa
import pyarrow.parquet as pq
import threading
import pandas as pd
import pytz
import os
import boto3

# Import AWS credentials from separate file
from aws_credentials import aws_access_key_id, aws_secret_access_key, aws_region, bucket_name

# Initialize the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)


# Initialize the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

 
 
# Define buffer and other parameters
buffer = []
buffer_lock = threading.Lock()
flush_interval = 200 
buffer_size = 15000
 
# Function to write buffer to AWS S3 in Parquet format
def flush_buffer_to_s3():
    if not buffer:
        return
    print("Flushing buffer to S3...")
    parquet_buffer = io.BytesIO()
    table = pa.Table.from_pydict({key: [row[key] for row in buffer] for key in buffer[0].keys()})
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)
 
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{current_time}_file.parquet"
 
    s3_client.upload_fileobj(parquet_buffer, bucket_name, s3_key)
    buffer.clear()
 
# Function to periodically flush the buffer
def periodic_flush():
    while True:
        time.sleep(flush_interval)
        with buffer_lock:
            flush_buffer_to_s3()
 
# Start the periodic flush in a separate thread
flush_thread = threading.Thread(target=periodic_flush, daemon=True)
flush_thread.start()
 
# Function to process each micro-batch
def foreach_batch_function(df, epoch_id):
    rows = df.collect()
    print(f"Processing {len(rows)} rows in micro-batch {epoch_id}")
    for row in rows:
        with buffer_lock:
            buffer.append(row.asDict())
            print(f"Buffer length after appending: {len(buffer)}")
 
 
def main():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

    # Initialize Spark session
    spark = SparkSession.builder.appName("FinalProject").getOrCreate()

    # Set log level to minimize verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    # Define the schema with StringType for all fields
    schema = StructType([
            StructField("index", IntegerType(), True),
            StructField("engine", IntegerType(), True),
            StructField("cycles", IntegerType(), True),
            StructField("alt", FloatType(), True),
            StructField("mach", FloatType(), True),
            StructField("TRA", FloatType(), True),
            StructField("T2", FloatType(), True),
            StructField("T24", FloatType(), True),
            StructField("T30", FloatType(), True),
            StructField("T50", FloatType(), True),
            StructField("P2", FloatType(), True),
            StructField("P15", FloatType(), True),
            StructField("P30", FloatType(), True),
            StructField("Nf", FloatType(), True),
            StructField("Nc", FloatType(), True),
            StructField("epr", FloatType(), True),
            StructField("Ps30", FloatType(), True),
            StructField("phi", FloatType(), True),
            StructField("NRf", FloatType(), True),
            StructField("NRc", FloatType(), True),
            StructField("BPR", FloatType(), True),
            StructField("farB", FloatType(), True),
            StructField("htBleed", IntegerType(), True),
            StructField("Nf_dmd", IntegerType(), True),
            StructField("PCNfR_dmd", FloatType(), True),
            StructField("W31", FloatType(), True),
            StructField("W32", FloatType(), True),
            StructField("source", IntegerType(), True)
])
 
    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'localhost:9092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                )
 
    shipDF = read_kafka_topic('demo', schema).alias('data')
 
    query = shipDF.writeStream.foreachBatch(foreach_batch_function).start()
    # Wait for the first batch to complete
    query.awaitTermination()  # Adjust this time as needed
 
if __name__ == "__main__":
    main()
