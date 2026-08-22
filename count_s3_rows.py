from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CountS3Rows") \
    .getOrCreate()

s3_path = "s3a://fraud-streaming-landing-zone/stream_transactions/"

df = spark.read.parquet(s3_path)

total_rows = df.count()

print(f"Total number of rows: {total_rows}")

spark.stop()
