import os
import sys
import shutil
# !pip install shapely pyspark
from shapely.geometry import Point, Polygon

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, udf, when, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType

input_data_path = f'../taxi-data'
base_output_path = f'Output'
checkpoint_path = f'Checkpoint'

goldman = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
citigroup = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140,40.720053], [-74.012083, 40.720267]]

# Clear the specified directory if it exists.
def clear_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)

# check if a point is inside a bounding box
def is_in_polygon(lon, lat, bounding_box):
    point = Point(lon, lat)
    polygon = Polygon(bounding_box)
    return polygon.contains(point)

is_goldman_udf = udf(lambda lon, lat: is_in_polygon(lon, lat, goldman), BooleanType())
is_citigroup_udf = udf(lambda lon, lat: is_in_polygon(lon, lat, citigroup), BooleanType())

# Write the result of each batch to the specified output path.
def writing_result(batch_df, batch_id):

    unique_end_times = batch_df.collect()

    for row in unique_end_times:
        end_time = row['end']
        citigroup_count = row['attend_citigroup_count']
        goldman_count = row['attend_goldman_count']
        output_file = os.path.join(base_output_path, f"output-{end_time.hour * 3600 * 1000}", "result")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(output_file, 'w') as file:
            file.write(f"(citigroup, {citigroup_count})\n")
            file.write(f"(goldman, {goldman_count})\n")

            
            
if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Invalid command. Use: spark-submit <file_path>", file=sys.stderr)
        sys.exit(-1)
        
    spark = SparkSession\
        .builder\
        .appName("Spark Streaming Region Event Count")\
        .getOrCreate()
        
    # Read all the csv files written atomically in a directory
    data_schema = StructType([
                            StructField("type", StringType(), True),
                            StructField("VendorID", StringType(), True),
                            StructField("pickup_datetime", TimestampType(), True),
                            StructField("dropoff_datetime", TimestampType(), True),
                            StructField("unused_feature1", StringType(), True),
                            StructField("unused_feature2", StringType(), True),
                            StructField("unused_feature3", DoubleType(), True),
                            StructField("unused_feature4", DoubleType(), True),
                            StructField("dropoff_longitude_green", DoubleType(), True),
                            StructField("dropoff_latitude_green", DoubleType(), True),
                            StructField("dropoff_longitude_yellow", DoubleType(), True),
                            StructField("dropoff_latitude_yellow", DoubleType(), True),   
                            ])
    
    df = (
        spark
        .readStream
        .option("sep", ",")
        .schema(data_schema)
        .csv(input_data_path) 
        )
        
    # Clear previous output and checkpoint directories
    clear_directory(base_output_path)
    clear_directory(checkpoint_path)

    # Event location filtering
    yellow_taxi_filtered_df = (
        df 
        .filter(col('type') == 'yellow')
        .withColumn('is_attending_goldman', is_goldman_udf(col('dropoff_longitude_yellow'), col('dropoff_latitude_yellow')))
        .withColumn('is_attending_citigroup', is_citigroup_udf(col('dropoff_longitude_yellow'), col('dropoff_latitude_yellow')))
        )
        
    green_taxi_filtered_df = (
        df 
        .filter(col('type') == 'green')
        .withColumn('is_attending_goldman', is_goldman_udf(col('dropoff_longitude_green'), col('dropoff_latitude_green')))
        .withColumn('is_attending_citigroup', is_citigroup_udf(col('dropoff_longitude_green'), col('dropoff_latitude_green')))
            )
    
    merged_df = yellow_taxi_filtered_df.union(green_taxi_filtered_df)
    merged_df = merged_df.withColumn('is_attending_goldman', merged_df['is_attending_goldman'].cast(BooleanType()))
    merged_df = merged_df.withColumn('is_attending_citigroup', merged_df['is_attending_citigroup'].cast(BooleanType()))
        
    # For Event Count        
    window_duration = "1 hour"
    window_df = (
        merged_df 
        .withWatermark("dropoff_datetime", "2 hour") 
        .groupBy(window("dropoff_datetime", window_duration))
        .agg(
            count(when(col('is_attending_goldman'), 1)).alias('attend_goldman_count'), 
            count(when(col('is_attending_citigroup'), 1)).alias('attend_citigroup_count')
            )
        .select(
            col('window.start').alias("start"),
            col('window.end').alias("end"),
            'attend_goldman_count',
            'attend_citigroup_count'
            )
        )
        
    query = (
        window_df
        .writeStream
        .foreachBatch(writing_result)
        .outputMode("complete")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 minutes")
        .start()
    )

    query.awaitTermination(timeout=100)

# Commands for TrendingArrivals directory:
# Run the code:
#   spark-submit RegionEventCount.py
# Note: Please wait about 300s for the program to run. The log is update in output.log