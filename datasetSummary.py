from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CountTransactionMetrics") \
    .getOrCreate()

s3_path = "s3a://fraud-streaming-landing-zone/stream_transactions/"

# Read Parquet files from S3
df = spark.read.parquet(s3_path)

# 1. Print schema
print("\n========== SCHEMA ==========")
df.printSchema()

# 2. Total transaction records
total_records = df.count()

# 3. Unique transactions
unique_transactions = df.select("trans_num").distinct().count()

# 4. Unique customers based on SSN
unique_customers = df.select("ssn").distinct().count()

# 5. Unique credit cards
unique_cards = df.select("cc_num").distinct().count()

# 6. Print results
print("\n========== DATASET SUMMARY ==========")
print(f"Total transaction records: {total_records:,}")
print(f"Unique transactions:       {unique_transactions:,}")
print(f"Unique customers (SSN):    {unique_customers:,}")
print(f"Unique cards (CC):         {unique_cards:,}")

spark.stop()
