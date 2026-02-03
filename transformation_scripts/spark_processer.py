from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, get_json_object, 
    from_unixtime, coalesce, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, TimestampType
)

# ClickHouse connection details
CH_URL = "jdbc:clickhouse://bsf5wsjr6d.asia-northeast1.gcp.clickhouse.cloud:8443/default?ssl=true"
CH_USER = "default"
CH_PASS = "Rj2hzzu_OUyLh"
CH_TABLE = "user_events"

# Kafka connection details
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "customer-activities"

# Define event schema based on your Excel data
event_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("customer", StringType(), True),  # JSON string
    StructField("customer_id", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("description", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("event", StringType(), True),
    StructField("extras", StringType(), True),  # JSON string
    StructField("ip_address", StringType(), True),
    StructField("key", StringType(), True),
    StructField("model_id", DoubleType(), True),
    StructField("model_type", StringType(), True),
    StructField("module", StringType(), True),
    StructField("package", StringType(), True),  # JSON string
    StructField("package_id", DoubleType(), True),
    StructField("updated_at", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("version", StringType(), True),
    StructField("view_type", StringType(), True),
])

# Create Spark Session
spark = SparkSession.builder \
    .appName("CustomerActivities-Kafka-to-ClickHouse") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "com.clickhouse:clickhouse-jdbc:0.6.5") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n🚀 Starting Customer Activities: Kafka to ClickHouse streaming...")

# Read from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✅ Connected to Kafka")

# Parse JSON data from Kafka and transform to match ClickHouse schema
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), event_schema).alias("data")) \
    .select("data.*")

# Transform data to match ClickHouse schema
transformed_df = parsed_df.select(
    # _id - Unique identifier (Primary Key)
    col("_id"),
    
    # created_at - Convert to timestamp (DateTime64(3) in ClickHouse)
    to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("created_at"),
    
    # customer_id - Convert to Long/Int64
    col("customer_id").cast(LongType()).alias("customer_id"),
    
    # customer_name - Extract from customer JSON
    get_json_object(col("customer"), "$.name").alias("customer_name"),
    
    # event - Event type (e.g., 'click')
    coalesce(col("event"), lit("unknown")).alias("event"),
    
    # key - Event key (e.g., 'download_progress')
    coalesce(col("key"), lit("")).alias("key"),
    
    # device_type - Device type (e.g., 'android')
    coalesce(col("device_type"), lit("")).alias("device_type"),
    
    # module - Module name (e.g., 'download')
    coalesce(col("module"), lit("")).alias("module"),
    
    # extras - Keep as JSON string
    coalesce(col("extras"), lit("{}")).alias("extras"),
    
    # description - Event description
    coalesce(col("description"), lit("")).alias("description")
)

# Function to write each batch to ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f"\n📦 Processing batch {batch_id} with {batch_df.count()} records...")
        
        # Remove duplicates within the batch based on _id (ensure uniqueness)
        deduped_df = batch_df.dropDuplicates(["_id"])
        deduped_count = deduped_df.count()
        
        if deduped_count < batch_df.count():
            print(f"⚠️  Removed {batch_df.count() - deduped_count} duplicate records")
        
        # Show sample data
        print("Sample records:")
        deduped_df.select("_id", "customer_id", "customer_name", "event", "key", "description") \
            .show(3, truncate=50)
        
        try:
            deduped_df.write \
                .format("jdbc") \
                .option("url", CH_URL) \
                .option("user", CH_USER) \
                .option("password", CH_PASS) \
                .option("dbtable", CH_TABLE) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("batchsize", "5000") \
                .option("rewriteBatchedStatements", "true") \
                .option("isolationLevel", "NONE") \
                .mode("append") \
                .save()
            
            print(f"✅ Batch {batch_id}: {deduped_count} unique records written to ClickHouse!")
            
        except Exception as e:
            print(f"❌ Error writing batch {batch_id}: {str(e)}")
            import traceback
            traceback.print_exc()
    else:
        print(f"⚠️  Batch {batch_id} is empty, skipping...")

# Write stream to ClickHouse
query = transformed_df \
    .writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✅ Streaming started! Waiting for data from Kafka...")
print(f"📊 Reading from topic: {KAFKA_TOPIC}")
print(f"💾 Writing to ClickHouse table: {CH_TABLE}")
print(f"🔑 Using ReplacingMergeTree - duplicate _id will be handled by ClickHouse")
print("\n⏸️  Press Ctrl+C to stop...\n")

# Wait for the streaming to finish (runs until interrupted)
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n🛑 Stopping streaming...")
    query.stop()
    spark.stop()
    print("✅ Streaming stopped successfully!")
