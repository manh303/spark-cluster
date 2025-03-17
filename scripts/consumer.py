from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Tạo Spark Session
spark = SparkSession.builder \
    .appName("KafkaSpark") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển đổi dữ liệu Kafka thành chuỗi JSON
df_parsed = df.selectExpr("CAST(value AS STRING)")

# Hiển thị dữ liệu trên terminal
query = df_parsed.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
