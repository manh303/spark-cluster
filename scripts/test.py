from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Example") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

# PostgreSQL connection configuration
jdbc_url = "jdbc:postgresql://postgres-db:5434/employee"
properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

try:
    # Read data from PostgreSQL table
    df = spark.read.jdbc(url=jdbc_url, table="employees", properties=properties)

    # Show data
    print("📊 Data from PostgreSQL:")
    df.show()

    # Save to CSV
    df.write.mode("overwrite").csv("/app/data/employees_output")
    print("✅ Data successfully saved to CSV")

except Exception as e:
    print(f"❌ Error: {str(e)}")
    
finally:
    # Stop SparkSession
    spark.stop()