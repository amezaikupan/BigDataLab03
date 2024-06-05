import os
import sys
import shutil

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, hour, col, count
from pyspark.sql.functions import split, date_format, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

input_data_path = f'../taxi-data'
base_output_path = f'Output'
checkpoint_path = f'Checkpoint'

# Clear the specified directory if it exists.
def clear_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)
        
# Generate output path based on window end time
def get_output_path(end_time):
    end_hour_ms = (end_time.hour * 3600 * 1000)
    return f'{base_output_path}/output-{end_hour_ms}'

# Write the result of each batch to the specified output path.
def writing_result(batch_df, batch_id):

    unique_end_times = batch_df.select('end').distinct().collect()

    for row in unique_end_times:
        end_time = row['end']
        filtered_batch = batch_df.filter(batch_df['end'] == end_time)
        
        if filtered_batch.count() > 0:
            output_path = get_output_path(end_time)
            filtered_batch.\
                coalesce(1).\
                write.mode('overwrite').\
                format('csv').\
                option('header', 'true').\
                save(output_path)
    
if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Invalid command. Use: spark-submit <file_path>", file=sys.stderr)
        sys.exit(-1)
        
    # Initialize a Spark session
    spark = (
        SparkSession
        .builder 
        .appName('Spark Streaming Event Count')
        .getOrCreate())

    # Define schema
    taxi_data_schema = StructType([
                                    StructField("type", StringType(), True),
                                    StructField("VendorID", StringType(), True),
                                    StructField("pickup_datetime", TimestampType(), True),
                                    StructField("dropoff_datetime", TimestampType(), True),
                                ])
    
    # Clear previous output and checkpoint directories
    clear_directory(base_output_path)
    clear_directory(checkpoint_path)

    # Read streaming data
    df = (
        spark 
        .readStream
        .option('sep', ',')
        .schema(taxi_data_schema)
        .csv(input_data_path) 
        )

    window_duration = "1 hour"
    
    # Aggregate event count per hou
    event_count_df = (
        df
        .withWatermark("dropoff_datetime", window_duration)
        .groupBy(window("dropoff_datetime", window_duration))
        .agg(count("*").alias("event_count"))
        .select(
            col('window.start').alias("start"),
            col('window.end').alias("end"),
            'event_count'
        )
    )

    # Start the streaming query
    query = (
        event_count_df
        .writeStream
        .foreachBatch(writing_result)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime='10 seconds')
        .start()
    )

    query.awaitTermination(timeout=100)

    spark.stop()

# Commands for TrendingArrivals directory:
# Run the code:
#   spark-submit EventCount.py
# Note: Please wait about 300s for the program to run. The log is update in output.log