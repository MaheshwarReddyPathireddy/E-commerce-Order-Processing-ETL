from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session

spark = SparkSession.builder \
    .appName("ShopFastETL") \
    .getOrCreate()

# MySQL connection info

mysql_url = "jdbc:mysql://localhost:3306/Shopfast"  # Replace DB name if different
mysql_properties = {
    "user": "root",                     # Replace with your MySQL username
    "password": "#Maheshwar____",       # Replace with your MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read MySQL tables

customers = spark.read.jdbc(url=mysql_url, table="customers", properties=mysql_properties)
orders = spark.read.jdbc(url=mysql_url, table="orders", properties=mysql_properties)
products = spark.read.jdbc(url=mysql_url, table="products", properties=mysql_properties)

# Read CSV files

payments = spark.read.option("header", True).csv("/Users/maheshwarreddy/Desktop/payments.csv")
inventory = spark.read.option("header", True).csv("/Users/maheshwarreddy/Desktop/inventory.csv")

# Handle duplicate columns

orders = orders.withColumnRenamed("status", "order_status")
payments = payments.withColumnRenamed("status", "payment_status")

# Join all data

sales = orders.join(customers, "customer_id", "left") \
              .join(products, "product_id", "left") \
              .join(payments, "order_id", "left") \
              .join(inventory, "product_id", "left")

#  Transformations
sales = sales.withColumn("total_price", col("quantity") * col("price"))

# Write consolidated table back to MySQL

sales.write.jdbc(url=mysql_url,
                 table="fact_sales",
                 mode="overwrite", 
                 properties=mysql_properties)

# Verification 
fact_sales = spark.read.jdbc(url=mysql_url, table="fact_sales", properties=mysql_properties)
fact_sales.show(5)

