import sys, string
import os
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, round, countDistinct, expr, unix_timestamp, udf
from pyspark.sql.types import FloatType, IntegerType, DoubleType
import subprocess
from datetime import datetime
import logging

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("BigDataCWPart2")\
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

    #reading the block data in
    blocks = spark.read.csv(f"s3a://{s3_data_repository_bucket}/ECS765/ethereum/blocks.csv", header=True, inferSchema=True)
    #reading the transaction data in
    transactions = spark.read.csv(f"s3a://{s3_data_repository_bucket}/ECS765/ethereum/transactions.csv", header=True, inferSchema=True)

    blocks.printSchema()
    transactions.printSchema()

    logger.info(f"PART2")

    #grouping the data to put the top miners at the top, ie highest block amount
    top_miners = blocks.groupBy("miner").sum("size").withColumnRenamed("sum(size)", "total_size").orderBy(col("total_size").desc())
    top_miners.show(10, truncate=False)

    logger.info(f"PART3")

    #unix to date format
    blocks = blocks.withColumn("formatted_date", date_format(from_unixtime("timestamp"), "yyyy-MM-dd"))
    blocks.select("timestamp", "formatted_date").show(10, truncate=False)

    logger.info(f"PART4")

    #adding block and transaction data together
    hash_join = transactions.join(blocks, transactions["block_hash"] == blocks["hash"], "inner")

    logger.info(f"The number of lines is: {hash_join.count()}")

    logger.info(f"PART5")

    #renaming to differentiate
    transactions = transactions.withColumnRenamed("hash","transaction_hash")

    #Amount of blocks produced and unique senders for each day of September 2015
    september_2015 = hash_join.filter((month(from_unixtime(col("timestamp"))) == 9) & (date_format(from_unixtime(col("timestamp")), "yyyy") == "2015"))
    unique_day = september_2015.groupBy(date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd").alias("formatted_date")).agg(count("block_hash").alias("block_count"), countDistinct("from_address").alias("unique_senders_count_number")).orderBy("formatted_date")
    unique_day.show(truncate=False)

    #naming the csv file
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H-%M-%S")
    path_q5 = f"s3a://{s3_bucket}/teaching_material/ECS765P/Q5_{date_time}"
    #writing the csv file out
    unique_day.coalesce(1).write.option('header', 'true').csv(path_q5)
    #completed second half of question 5 separately - see task2part5.py
    #to create the csv - ccc method bucket ls teaching_material/ECS765P to get the csv file name
    #ccc method bucket cp -r bkt: file name

    logger.info(f"PART6")

    #total transactions when the index is 0 for october 2015
    october_2015 = hash_join.filter((month(from_unixtime(col("timestamp"))) == 10) & (date_format(from_unixtime(col("timestamp")), "yyyy") == "2015") & (col("transaction_index") == 0))
    october_total_fee = october_2015.withColumn("transaction_fee", col("gas") * col("gas_price")).groupBy(date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd").alias("formatted_date")).agg(sum("transaction_fee").alias("total_transaction_fee")).orderBy("formatted_date")
    october_total_fee.show(truncate=False)
    
    path_q6 = f"s3a://{s3_bucket}/teaching_material/ECS765P/Q6_{date_time}"
    october_total_fee.coalesce(1).write.option('header', 'true').csv(path_q6)
    #completed second half of question 6 separately - see task2part6.py
    #to create the csv - ccc method bucket ls teaching_material/ECS765P to get the csv file name
    #ccc method bucket cp -r bkt: file name
    

    spark.stop()