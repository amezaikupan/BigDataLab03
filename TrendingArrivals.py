import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, hour, col, count, udf
from pyspark.sql.functions import split, date_format, window, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType, BooleanType
# from pyspark.sql.streaming.DataStreamWriter import outputMode


# UDF to check if a point is inside a polygon
def point_inside_polygon_udf(x, y, poly):
    def point_inside_polygon(x_val, y_val):
        n = len(poly)
        inside = False

        p1x, p1y = poly[0]
        for i in range(n + 1):
            p2x, p2y = poly[i % n]
            if y_val > min(p1y, p2y):
                if y_val <= max(p1y, p2y):
                    if x_val <= max(p1x, p2x):
                        if p1y != p2y:
                            xints = (y_val - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x_val <= xints:
                            inside = not inside
            p1x, p1y = p2x, p2y

        return inside
    
    # Apply the inner function to each row of the DataFrame
    return udf(point_inside_polygon, BooleanType())(x, y)

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: structured_network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
        
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', handlers=[logging.FileHandler("/home/april/BigDataLab03/TrendingArrivals/output.log"), logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger()
        
    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()
        
    # Increase the maximum number of rows to display
    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 1000)  # Adjust 1000 to the number of rows you want to display
    
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
    
    df = spark \
        .readStream \
        .option("sep", ",") \
        .schema(userSchema) \
        .csv("/home/april/taxi-data")\
        
    # Event location filtering
    # Location
    goldman = [(-74.0141012, 40.7152191), (-74.013777, 40.7152275), (-74.0141027, 40.7138745), (-74.0144185, 40.7140753)]
    citigroup = [(-74.011869, 40.7217236), (-74.009867, 40.721493), (-74.010140,40.720053), (-74.012083, 40.720267)]
    yellow_taxi_filtered_df = df \
        .filter(col('type') == 'yellow')\
        .withColumn('isAttendingGoldman', 
            point_inside_polygon_udf(col('dropoff_longitude_yellow'), col('dropoff_latitude_yellow'), goldman))\
        .withColumn('isAttendingCitigroup',
            point_inside_polygon_udf(col('dropoff_longitude_yellow'), col('dropoff_latitude_yellow'), citigroup))
        
    green_taxi_filtered_df = df \
        .filter(col('type') == 'green')\
        .withColumn('isAttendingGoldman', 
            point_inside_polygon_udf(col('dropoff_longitude_green'), col('dropoff_latitude_green'), goldman))\
        .withColumn('isAttendingCitigroup',
            point_inside_polygon_udf(col('dropoff_longitude_green'), col('dropoff_latitude_green'), citigroup))
    
    merged_df = yellow_taxi_filtered_df.union(green_taxi_filtered_df)

    merged_df = merged_df.withColumn("isAttendingGoldman", merged_df["isAttendingGoldman"].cast(BooleanType()))
    merged_df = merged_df.withColumn("isAttendingCitigroup", merged_df["isAttendingCitigroup"].cast(BooleanType()))
    
    # merged_df = merged_df.filter(col('isAttendingRegionalEvent') == True)
        
    # For Event Count        
    window_duration = "10 minutes"
    window_df = merged_df \
        .withWatermark("dropoff_datetime", '4 hour') \
        .groupBy(window("dropoff_datetime", window_duration))\
        .agg(count(when(col("isAttendingGoldman"), 1)).alias("attendGoldmanCount"), 
            count(when(col("isAttendingCitigroup"), 1)).alias("attendCitigroupCount"))\
        .orderBy('window')\
        
    output_path = f"/home/april/BigDataLab03/EventCountOutput/output-data"
    checkpoint_path = f"/home/april/BigDataLab03/EventCountOutput/checkpoint"
    
    # query = window_df\
    #     .writeStream\
    #     .outputMode("append")\
    #     .option("path", output_path) \
    #     .option("checkpointLocation", checkpoint_path)\
    #     .start()
    
    
    # Detect spikes
    def detect_spikes(batch_df, batch_id):
        previous_state={}     
        
        if not batch_df.isEmpty():
            current_counts = batch_df.collect()
            first_row = current_counts[0]
            previous_state[first_row['window']['start']] = {'goldman': 0, 'citigroup': 0}
            
            # print(current_counts)
            for row in current_counts:
                window_start = row['window']['start']
                window_end = row['window']['end']
                goldman_count = row['attendGoldmanCount']
                citigroup_count = row['attendCitigroupCount']
                
                
                if window_end not in previous_state:
                    previous_state[window_end] = {'goldman': 0, 'citigroup': 0}
                
                goldman_spike = goldman_count >= 2 * previous_state[window_start]['goldman']
                citigroup_spike = citigroup_count >= 2 * previous_state[window_start]['citigroup']
                
                if goldman_spike and goldman_count >= 10:
                    print(f"The number of arrivals to Goldman Sachs has doubled from {previous_state[window_start]['goldman']} to {goldman_count} at {window_end}")
                    # logger.info(f"The number of arrivals to Goldman Sachs has doubled from {previous_state[window_end]['goldman']} to {goldman_count} at {window_end}")
                    
                if citigroup_spike and citigroup_count >= 10:
                    print(f"The number of arrivals to Citigroup has doubled from {previous_state[window_start]['citigroup']} to {citigroup_count} at {window_end}")
                    # logger.info(f"The number of arrivals to Citigroup has doubled from {previous_state[window_end]['citigroup']} to {citigroup_count} at {window_end}")
                    
                previous_state[window_end]['goldman'] = goldman_count
                previous_state[window_end]['citigroup'] = citigroup_count               
                

    # Use foreachBatch to apply the spike detection logic
    # query = window_df \
    #     .writeStream \
    #     .outputMode("update") \
    #     .foreachBatch(detect_spikes) \
    #     .start()
    
    # window_df.writeStream.format("csv").option("checkpointLocation", checkpoint_path).toTable("myTable").trigger(processingTime="10 minutes")

    
    query = window_df\
            .writeStream\
            .outputMode("complete")\
            .format('console')\
            .option("truncate", "false") \
            .option('numRows', 100000)\
            .trigger(processingTime="10 minutes")\
            .start()

    # change_query = window_df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("csv") \
    #     .option("path", output_path) \
    #     .option("checkpointLocation", checkpoint_path) \
    #     .option("truncate", "false") \
    #     .trigger(processingTime="10 minutes") \
    #     .start()

    # change_query.awaitTermination()
    query.awaitTermination(timeout=600)
    
    
    # window_df.writeStream.format("csv").option("checkpointLocation", checkpoint_path).toTable("myTable").trigger(processingTime="10 minutes")

    
    # query = window_df\
    #         .writeStream\
    #         .outputMode("complete")\
    #         .format('console')\
    #         .option("truncate", "false") \
    #         .foreachBatch(detect_spikes) \
    #         .trigger(processingTime="10 minutes")\
    #         .start()

    # # change_query = window_df \
    # #     .writeStream \
    # #     .outputMode("append") \
    # #     .format("csv") \
    # #     .option("path", output_path) \
    # #     .option("checkpointLocation", checkpoint_path) \
    # #     .option("truncate", "false") \
    # #     .trigger(processingTime="10 minutes") \
    # #     .start()

    # # change_query.awaitTermination()
    # query.awaitTermination(timeout=600)