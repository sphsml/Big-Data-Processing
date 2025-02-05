import sys, string
import os
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, round
from pyspark.sql.types import FloatType, IntegerType
from datetime import datetime
import logging

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("BigDataCW")\
        .getOrCreate()

    logger4j= spark._jvm.org.apache.log4j
    spark.sparkContext.setLogLevel("INFO")
    logger = logger4j.LogManager.getLogger(__name__)
    
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    logger.info("START OF PROGRAM HERE")
    logger.info(f"PART1")
    logger.info(f"READING")

    #loading all months in
    months_2023 = spark.read.csv(f"s3a://{s3_data_repository_bucket}/ECS765/nyc_taxi/yellow_tripdata/2023/yellow_tripdata_2023-*.csv", header=True, inferSchema=True)
    
    logger.info(f"FINISHED READING")
    #outputting count
    logger.info(f"The number of entries is: {months_2023.count()}")

    logger.info(f"PART2")

    #fares costing more than 50 but a distance of less than 1 mile in the first week of Febuary 2023
    fare_and_distance = months_2023.filter((col("fare_amount") > 50) & (col("trip_distance") < 1) & (col("tpep_pickup_datetime") >= "2023-02-01") & (col("tpep_pickup_datetime") <= "2023-02-07"))

    fad_sort = (fare_and_distance
                 .withColumn("dates", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
                 .groupBy("dates")
                 .count()
                 .orderBy("dates"))
    
    fad_sort.show(truncate=False)

    logger.info(f"PART3")

    #loading zone data in
    zone_data = spark.read.csv(f"s3a://{s3_data_repository_bucket}/ECS765/nyc_taxi/taxi_zone_lookup.csv", header=True, inferSchema=True)

    #joining the pickup data with the zone data
    pickup_rides = months_2023.join(
        zone_data,
        months_2023["PULocationID"] == zone_data["LocationID"],
        "left"
    ).withColumnRenamed("Borough", "Pickup_Borough") \
     .withColumnRenamed("Zone", "Pickup_Zone") \
     .withColumnRenamed("service_zone", "Pickup_service_zone") \
     .drop("LocationID", "PULocationID")

    #adding the dropoff data
    pickups_dropoffs = pickup_rides.join(
        zone_data,
        pickup_rides["DOLocationID"] == zone_data["LocationID"],
        "left"
    ).withColumnRenamed("Borough", "Dropoff_Borough") \
     .withColumnRenamed("Zone", "Dropoff_Zone") \
     .withColumnRenamed("service_zone", "Dropoff_service_zone") \
     .drop("LocationID", "DOLocationID")
    
    pickups_dropoffs.printSchema()

    logger.info(f"PART4")

    #adding the route column
    pickups_dropoffs =pickups_dropoffs.withColumn("route", concat_ws(" to ", "Pickup_Borough", "Dropoff_Borough"))
                                                  #adding the month column
    pickups_dropoffs = pickups_dropoffs.withColumn("Month", month("tpep_pickup_datetime"))
    pickups_dropoffs.show(10, truncate=False)

    logger.info(f"PART5")

    #grouping the data by month and route and adding the sum of tips and passengers
    grouped_data = pickups_dropoffs.groupBy("Month", "route").agg(round(sum("tip_amount"), 2).alias("total_tip_amount"), sum("passenger_count").alias("total_passenger_count"))

    tip_avg = grouped_data.withColumn("average_tip_per_passenger", round(col("total_tip_amount") / col("total_passenger_count"), 2))

    tip_avg.show(10, truncate = False)

    logger.info(f"PART6")

    #filtering to find the passengers who didnt tip
    no_tip = tip_avg.filter(col("average_tip_per_passenger") == 0)
    no_tip.show(truncate = False)

    logger.info(f"PART7")

    #ordering the data by the top tippers
    top_tips = tip_avg.orderBy(col("average_tip_per_passenger").desc())
    top_tips.show(10, truncate=False)
    
    spark.stop()