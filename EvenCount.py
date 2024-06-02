import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, hour, col, count
from pyspark.sql.functions import split, date_format, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
# from pyspark.sql.streaming.DataStreamWriter import outputMode

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
        
    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()
        
    # spark.sparkContext.setLogLevel("ERROR") 
    # Read all the csv files written atomically in a directory
    userSchema = StructType([
    StructField("type", StringType(), True),
    StructField("VendorID", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    ])
    
    df = spark \
        .readStream \
        .option("sep", ",") \
        .schema(userSchema) \
        .csv("/home/april/taxi-data")  
        
        
    # Version 1: Working ver   
    # window_duration = "1 hour"
    # window_df = df \
    #     .withWatermark("dropoff_datetime", window_duration) \
    #     .groupBy(window("dropoff_datetime", window_duration))\
    #     .agg(count("*")\
    #     .alias("event_count"))
    #     # .orderBy('window')\
        
    
    # # query = window_df\
    # #     .writeStream\
    # #     .outputMode("append")\
    # #     .option("path", output_path) \
    # #     .option("checkpointLocation", checkpoint_path)\
    # #     .start()
        
    # # query = window_df\
    # #     .writeStream\
    # #     .outputMode("complete")\
    # #     .format('console')\
    # #     .option("truncate", "false") \
    # #     .trigger(processingTime="10 minutes")\
    # #     .start()

    # write_query = window_df.select(
    #     'window.start',
    #     'window.end',
    #     'event_count'
    # ).writeStream.format('csv')\
    #     .option('path', output_path)\
    #     .option('checkpointLocation', checkpoint_path)\
    #     .trigger(availableNow=True) \
    #     .start()
        
    # # query.awaitTermination(timeout=600)
    # write_query.awaitTermination(timeout=60)
    # ------
    
    # Version 2: 
    output_path = f"/home/april/BigDataLab03/EventCountOutput/output-data"
    checkpoint_path = f"/home/april/BigDataLab03/EventCountOutput/checkpoint"

    window_duration = "1 hour"
    query = df \
        .withWatermark("dropoff_datetime", window_duration) \
        .groupBy(window("dropoff_datetime", window_duration))\
        .agg(count("*")\
        .alias("event_count"))\
        .select(
            'window.start',
            'window.end',
            'event_count'
        ).writeStream.format('csv')\
            .option('path', output_path)\
            .option('checkpointLocation', checkpoint_path)\
            .option("maxRecordsPerFile", 1) \
            .option("minRecordsPerFile", 1) \
            .option("batchsize", 1)\
            .option("outputFileName", "output_file")\
            .trigger(processingTime='10 seconds')\
            .start()
            
    
    query.awaitTermination(timeout=600)
    