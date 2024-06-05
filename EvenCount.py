import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, hour, col, count
from pyspark.sql.functions import split, date_format, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

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
        
    output_path = f"/home/april/BigDataLab03/EventCountOutput/output-data"
    checkpoint_path = f"/home/april/BigDataLab03/EventCountOutput/checkpoint"

    window_duration = "1 hour"
    
    def func(batch_df, batch_id):
        print("Batch id:", batch_id)
        print("--------------------------------------")
        for row in batch_df.collect():
            print(row)
        print("--------------------------------------")
        
    query = df \
        .withWatermark("dropoff_datetime", window_duration) \
        .groupBy(window("dropoff_datetime", window_duration))\
        .agg(count("*")\
        .alias("event_count"))\
        .select(
            'window.start',
            'window.end',
            'event_count'
        ).writeStream\
            .trigger(processingTime='10 seconds')\
            .foreachBatch(func)\
            .start()            
    
    query.awaitTermination(timeout=600)
    