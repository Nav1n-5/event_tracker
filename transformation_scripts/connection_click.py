from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InternalConnectionTest") \
    .getOrCreate()

print("\n--- Testing Kafka Connection ---")
try:
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "user_events") \
        .load()
    print("✅ Kafka Connection: SUCCESS")
except Exception as e:
    print(f"❌ Kafka Connection: FAILED \n{e}")

print("\n--- Testing ClickHouse Connection ---")
try:
    # Using the standard JDBC format
    df_ch = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", "default") \
        .option("password", "pass") \
        .option("query", "SELECT 1 AS test") \
        .load()
    df_ch.show()
    print("✅ ClickHouse Connection: SUCCESS")
except Exception as e:
    print(f"❌ ClickHouse Connection: FAILED \n{e}")

spark.stop()