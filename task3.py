import sys, string
import os
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, round, countDistinct, expr, unix_timestamp, udf, array, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import subprocess
from datetime import datetime
from graphframes import GraphFrame
import logging

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("BigDataCWPart3")\
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
    months_2023 = spark.read.csv(f"s3a://{s3_data_repository_bucket}/ECS765/nyc_taxi/green_tripdata/2023/green_tripdata_2023-*.csv", header=True, inferSchema=True)
    
    logger.info(f"FINISHED READING")
    logger.info(f"The number of entries is: {months_2023.count()}")

    logger.info(f"PART2")

    #defining the schema
    vertexSchema = StructType([
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)])

    #loading zone data in
    vertices = spark.read.csv(f"s3a://{s3_data_repository_bucket}/ECS765/nyc_taxi/taxi_zone_lookup.csv", header=True, schema=vertexSchema)

    #renaming the locationID to ID to create a key
    vertices = vertices.withColumnRenamed("LocationID", "id")
    edgeSchema = StructType([
        StructField("src", IntegerType(), True),
        StructField("dst", IntegerType(), True)
    ])

    #creating the start and destination locations from the months data
    edges = months_2023.select(col("PULocationID").alias("src"), col("DOLocationID").alias("dst"))

    vertices.show(5, truncate=False)
    edges.show(5, truncate=False)

    logger.info(f"PART3")

    #transforming the data into triplet graphs
    graph = GraphFrame(vertices, edges)

    triplets = graph.triplets

    triplets.show(10, truncate=False)

    logger.info(f"PART4")

    #filtering to find all connected vertices with the same borough and destination service zone
    connected_vertices = triplets.filter((col("src.Borough")==col("dst.Borough")) & (col("src.service_zone") == col("dst.service_zone")))

    connected_vertices = connected_vertices.select(col("src.id").alias("src_id"), col("dst.id").alias("dst_id"),
        col("src.Borough").alias("Borough"),
                                                   col("src.service_zone").alias("service_zone"))

    total_count = connected_vertices.count()
    logger.info(f"count: {total_count}")

    connected_vertices.show(10, truncate=False)

    logger.info(f"PART5")

    #finding shortest paths from each vertices to landmark with locationID 1
    shortest_paths = graph.shortestPaths(landmarks=["1"])

    shortest_paths = shortest_paths.select(col("id").alias("id_to_1"),explode(col("distances")).alias("landmark", "shortest_distance")).filter(col("landmark")=="1").select("id_to_1", "shortest_distance")

    shortest_paths.show(10, truncate=False)

    logger.info(f"PART6")

    #page ranking
    page_rank = graph.pageRank(resetProbability = 0.17, tol=0.01)

    page_rank_result = page_rank.vertices.select(col("id"), col("pagerank")).orderBy(col("pagerank").desc())
    page_rank_result.show(5, truncate=False)

    spark.stop()