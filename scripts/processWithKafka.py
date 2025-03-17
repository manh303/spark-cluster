from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Tạo Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Định nghĩa Schema JSON
schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Đọc dữ liệu từ Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển đổi dữ liệu
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Tính tổng doanh thu
df_revenue = df_parsed.withColumn("total_revenue", col("quantity") * col("price"))

# Hiển thị kết quả
query = df_revenue.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
