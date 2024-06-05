import os
import sys
import shutil
import logging
from shapely.geometry import Point, Polygon

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, udf, window, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType

# Clear the specified directory if it exists.
def clear_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)

def is_in_polygon(lon, lat, bounding_box):
    point = Point(lon, lat)
    polygon = Polygon(bounding_box)
    return polygon.contains(point)

goldman = [(-74.0141012, 40.7152191), (-74.013777, 40.7152275), (-74.0141027, 40.7138745), (-74.0144185, 40.7140753)]
citigroup = [(-74.011869, 40.7217236), (-74.009867, 40.721493), (-74.010140,40.720053), (-74.012083, 40.720267)]

is_goldman_udf = udf(lambda lon, lat: is_in_polygon(lon, lat, goldman), BooleanType())
is_citigroup_udf = udf(lambda lon, lat: is_in_polygon(lon, lat, citigroup), BooleanType())

# Detect spikes
def detect_spikes (batch_df, batch_id, previous_state={}):

    if not batch_df.isEmpty():
        current_counts = batch_df.collect()

        for row in current_counts:
            window_start = row['window']['start']
            window_end = row['window']['end']
            goldman_count = row['attendGoldmanCount']
            citigroup_count = row['attendCitigroupCount']
            
            if window_start not in previous_state:
                previous_state[window_end] = {'goldman': goldman_count, 'citigroup': citigroup_count}
            else:
            
                goldman_spike = goldman_count >= 2 * previous_state[window_start]['goldman']
                citigroup_spike = citigroup_count >= 2 * previous_state[window_start]['citigroup']
                
                if goldman_spike and goldman_count >= 10:
                    time_stamp = window_end.hour * 3600 * 1000

                    logger.info(f"The number of arrivals to Goldman Sachs has doubled from {previous_state[window_start]['goldman']} to {goldman_count} at {time_stamp}")

                    output_file = os.path.join(output_path, f"part-{time_stamp}", "result")
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    with open(output_file, 'w') as file:
                        file.write(f"(goldman,({goldman_count},{time_stamp},{previous_state[window_start]['goldman']}))\n")
                    
                if citigroup_spike and citigroup_count >= 10:
                    time_stamp = window_end.hour * 3600 * 1000

                    logger.info(f"The number of arrivals to Citigroup has doubled from {previous_state[window_start]['citigroup']} to {citigroup_count} at {time_stamp}")

                    output_file = os.path.join(output_path, f"part-{time_stamp}", "result")
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    with open(output_file, 'w') as file:
                        file.write(f"(citigroup,({citigroup_count},{time_stamp},{previous_state[window_start]['citigroup']}))\n")

                previous_state[window_end] = {'goldman': goldman_count, 'citigroup': citigroup_count}

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Invalid command, use: spark-submit <.py file_path> --input <input_path> --checkpoint <checkpoint_path> --output <output_path>", file=sys.stderr)
        sys.exit(-1)
        
    input_path = sys.argv[sys.argv.index("--input") + 1] 
    checkpoint_path = sys.argv[sys.argv.index("--checkpoint") + 1]
    output_path = sys.argv[sys.argv.index("--output") + 1]

    clear_directory(output_path)
    clear_directory(checkpoint_path)

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', handlers=[logging.FileHandler("/home/caokhoi/spark-3.5.1-bin-hadoop3/BigDataLab03/TrendingArrivals/output.log"), logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger()
        
    spark = (
        SparkSession
        .builder
        .appName("Spark Streaming Trending Arrivals")
        .getOrCreate()
        )
        
    # Read all the csv files written atomically in a directory
    userSchema = StructType([
                            StructField("type", StringType(), True),
                            StructField("VendorID", StringType(), True),
                            StructField("pickup_datetime", TimestampType(), True),
                            StructField("dropoff_datetime", TimestampType(), True),
                            StructField("unused_feature1", StringType(), True),
                            StructField("unused_feature2", StringType(), True),
                            StructField("pickup_longitude", DoubleType(), True),
                            StructField("pickup_latitude", DoubleType(), True),
                            StructField("dropoff_longitude_green", DoubleType(), True),
                            StructField("dropoff_latitude_green", DoubleType(), True),
                            StructField("dropoff_longitude_yellow", DoubleType(), True),
                            StructField("dropoff_latitude_yellow", DoubleType(), True),   
                        ])
    
    df = (
        spark 
        .readStream
        .option("sep", ",") 
        .schema(userSchema) 
        .csv(input_path)
        )
        
    yellow_taxi_filtered_df = (
        df 
        .filter(col('type') == 'yellow')
        .withColumn('isAttendingGoldman', is_goldman_udf(col('dropoff_longitude_yellow'), col('dropoff_latitude_yellow')))
        .withColumn('isAttendingCitigroup', is_citigroup_udf(col('dropoff_longitude_yellow'), col('dropoff_latitude_yellow')))
        )
        
    green_taxi_filtered_df = (
        df 
        .filter(col('type') == 'green')
        .withColumn('isAttendingGoldman', is_goldman_udf(col('dropoff_longitude_green'), col('dropoff_latitude_green')))
        .withColumn('isAttendingCitigroup', is_citigroup_udf(col('dropoff_longitude_green'), col('dropoff_latitude_green')))
            )
    
    merged_df = yellow_taxi_filtered_df.union(green_taxi_filtered_df)

    merged_df = merged_df.withColumn('isAttendingGoldman', merged_df['isAttendingGoldman'].cast(BooleanType()))
    merged_df = merged_df.withColumn('isAttendingCitigroup', merged_df['isAttendingCitigroup'].cast(BooleanType()))
    
    # For Event Count        
    window_duration = '10 minutes'

    window_df = (
        merged_df
        .withWatermark('dropoff_datetime', '4 hours')
        .groupBy(window('dropoff_datetime', window_duration))
        .agg(
            count(when(col('isAttendingGoldman'), 1)).alias('attendGoldmanCount'), 
            count(when(col('isAttendingCitigroup'), 1)).alias('attendCitigroupCount')
            )
        .orderBy('window')
        )
        

    query = (
        window_df
        .writeStream
        .outputMode('complete')
        .format('console')
        .foreachBatch(detect_spikes)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime='10 minutes')
        .start()
        )

    query.awaitTermination(timeout=300)

# Commandline, from TrendingArrivals directory:
#  spark-submit TrendingArrivals.py --input ../taxi-data --checkpoint Checkpoint --output Output &> output.log

# cat Output/part-*/* | grep "(citigroup" 