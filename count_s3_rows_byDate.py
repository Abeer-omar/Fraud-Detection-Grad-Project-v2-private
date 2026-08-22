from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CountS3RowsByDate") \
    .getOrCreate()

s3_path = "s3a://fraud-streaming-landing-zone/stream_transactions/"

df = spark.read.parquet(s3_path)

print("Total number of rows:", df.count())

df.groupBy("trans_date") \
    .count() \
    .orderBy("trans_date") \
    .show(100, truncate=False)

spark.stop()
