from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Kết nối Spark
spark = SparkSession.builder.appName("KafkaToPostgreSQL").getOrCreate()

# Định nghĩa schema
schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True)
])

# Đọc dữ liệu Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_revenue = df_parsed.withColumn("total_revenue", col("quantity") * col("price"))

# Ghi vào PostgreSQL
df_revenue.writeStream \
    .foreachBatch(lambda batchDF, _: batchDF.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/sales_db") \
        .option("dbtable", "sales_revenue") \
        .option("user", "postgres") \
        .option("password", "1234") \
        .mode("append") \
        .save()) \
    .start().awaitTermination()
