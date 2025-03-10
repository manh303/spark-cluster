from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc

# Create SparkSession
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

# PostgreSQL connection configuration
jdbc_url = "jdbc:postgresql://postgres-db:5432/employee"
properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

try:
    # Read data from PostgreSQL
    df_sale = spark.read.jdbc(url=jdbc_url, table="sales_data", properties=properties)
    df_product = spark.read.jdbc(url=jdbc_url, table="products", properties=properties)
    df_category = spark.read.jdbc(url=jdbc_url, table="categories", properties=properties)
    
    # Calculate total quantity by product
    print("\n📊 Total Quantity by Product:")
    total_quantity_df = df_sale.groupBy("product_id") \
                              .agg(sum("quantity").alias("total_quantity")) \
                              .orderBy(desc("total_quantity"))
    total_quantity_df.show()
    
    # Calculate total revenue by product
    print("\n💰 Total Revenue by Product:")
    total_revenue_df = df_sale.groupBy("product_id") \
                             .agg(sum("revenue").alias("total_revenue")) \
                             .orderBy(desc("total_revenue"))
    total_revenue_df.show()
    
    # Calculate total revenue by category
    print("\n📈 Total Revenue by Category:")
    total_revenue2_df = df_sale.groupBy("category_id") \
                              .agg(sum("revenue").alias("total_revenue")) \
                              .orderBy(desc("total_revenue"))
    total_revenue2_df.show()
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
    print(f"Detailed error: {e.__cause__}")
    
finally:
    # Stop SparkSession
    spark.stop()