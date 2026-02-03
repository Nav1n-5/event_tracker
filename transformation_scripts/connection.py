from pyspark.sql import SparkSession

# FIXED: Changed jdbc:clickhouse:https:// to jdbc:clickhouse://
CH_URL = "jdbc:clickhouse://bsf5wsjr6d.asia-northeast1.gcp.clickhouse.cloud:8443/system?ssl=true"
CH_USER = "default"
CH_PASS = "Rj2hzzu_OUyLh"

spark = SparkSession.builder \
    .appName("ClickHouse-Connection-Test") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.6.5") \
    .getOrCreate()

print("\n--- 🔎 STARTING CONNECTION TEST ---")

try:
    # FIXED: Changed .options() to individual .option() calls
    test_df = spark.read \
        .format("jdbc") \
        .option("url", CH_URL) \
        .option("dbtable", "build_options") \
        .option("user", CH_USER) \
        .option("password", CH_PASS) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .load()
    
    print("✅ SUCCESS: Spark successfully connected to ClickHouse Cloud!")
    test_df.select("name", "value").show(5)
    
except Exception as e:
    print("\n❌ CONNECTION FAILED")
    print(f">>> ERROR DETAILS: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()