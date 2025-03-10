from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Example") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

# PostgreSQL connection configuration
jdbc_url = "jdbc:postgresql://localhost:5434/employee"
properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

try:
    # Read data from PostgreSQL table
    # df = spark.read.jdbc(url=jdbc_url, table="employees", properties=properties)
    # df_employees = spark.read.jdbc(url=jdbc_url, table="employees", properties=properties)
    # df_departments = spark.read.jdbc(url=jdbc_url, table="departments", properties=properties)

    # # Show data
    # # df_joined = df_employees.join(df_departments, df_employees.department == df_departments.department_name)
    # high_salary_employees = df.filter(df.salary >= 70000)
    # high_salary_employees.show()
    
    new_employee = [(4, "Eva", "Engineering", 95000),
            (5, "Frank", "Sales", 70000)]
    new_employee_df = spark.createDataFrame(new_employee, ["id", "name", "department", "salary"])
    new_employee_df.write.jdbc(url=jdbc_url, table="employees", mode="append", properties=properties)
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
    
finally:
    # Stop SparkSession
    spark.stop()