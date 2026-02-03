from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Schema Definition (Matches your Producer)
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_timestamp", StringType(), True)
])

spark = SparkSession.builder \
    .appName("KafkaToClickHouseStreaming") \
    .getOrCreate()

# 2. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Transformation Layer (Parsing JSON & Cleaning)
# We convert the binary 'value' column from Kafka into a readable object
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Optional: Clean timestamps to actual Timestamp types
cleaned_df = parsed_df.withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))

# 4. Sink to ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "user_events") \
        .option("user", "default") \
        .option("password", "pass") \
        .mode("append") \
        .save()

query = cleaned_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .start()

query.awaitTermination()