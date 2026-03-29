1.Load and Transform Data:
# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

df = spark.read.csv("/datasets/customers.csv", header=True, inferSchema=True)

df_filtered = df.filter((df.purchase_amount > 100) & (df.age >= 30))

df_result = df_filtered.select("customer_id", "name", "purchase_amount")

# Display result
display(df_result)

2.Handling Null Values:
# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#Copy the starter code or load the file path available in the problem statement 
df = spark.read.csv("/datasets/customers_raw.csv", header=True, inferSchema=True)

df_result = df.filter(
    (df.customer_id.isNotNull()) & (df.email.isNotNull())
)
# Display the final DataFrame using the display() function.
display(df_result)

3.Total Purchases by Customer:
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create Spark session
spark = SparkSession.builder.appName("Spark Playground").getOrCreate()
df = spark.read.csv("/datasets/customer_purchases.csv", header=True, inferSchema=True)
df_result = df.groupBy("customer_id") \
              .agg(sum("purchase_amount").alias("total_purchase")) \
              .orderBy("customer_id")

# Display the result
display(df_result)

4.Running Payroll:

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Assume the dataframes employees, payroll are already initialized.
df = employees.join(payroll, on="employee_id")

df_result = df.withColumn(
    "pay",
    F.when(
        F.col("hours_worked") <= 40,
        F.col("hours_worked") * F.col("hourly_rate")
    ).otherwise(
        (40 * F.col("hourly_rate")) +
        ((F.col("hours_worked") - 40) * F.col("hourly_rate") * 1.5)
    )
)

df_result = df_result.select("employee_id", "name", "position", "pay")

# Write the logic and display the final dataframe

display(df_result)


5.Food and Beverage Sales:

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Assume the dataframes products, sales, inventory are already initialized.

# Write the logic and display the final dataframe
from pyspark.sql import functions as F

# total sales
s = sales.groupBy("product_id").sum("quantity", "revenue")

# total stock
i = inventory.groupBy("product_id").sum("stock")

# join all
df_result = products.join(s, "product_id", "left") \
                    .join(i, "product_id", "left") \
                    .fillna(0)

# select final columns
df_result = df_result.select(
    "product_id",
    "name",
    "category",
    F.col("sum(quantity)").alias("total_quantity"),
    F.col("sum(revenue)").alias("total_revenue"),
    F.col("sum(stock)").alias("total_stock")
)

display(df_result)
display(df_result)
    
