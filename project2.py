import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, month, year, desc, round, to_date

print("🚀 All libraries imported successfully.")

print("\n📝 Skipping mock CSV file generation...")

print("\n🛠️ Starting PySpark Analysis...")

spark = SparkSession.builder.appName("EcommerceAnalysis").getOrCreate()
print("-> SparkSession initialized.")

orders = spark.read.csv("sample/orders.csv", header=True, inferSchema=True)
products = spark.read.csv("sample/products.csv", header=True, inferSchema=True)
users = spark.read.csv("sample/users.csv", header=True, inferSchema=True)

orders = orders.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
print("-> DataFrames loaded and cleaned.")

df = orders.join(products, "product_id", "inner").join(users, "user_id", "inner")
df.cache()
print("-> DataFrames joined successfully.")
print("\n--- Joined DataFrame Schema ---")
df.printSchema()

print("\n📈 Calculating Business Metrics...")

print("\n--- Total Revenue by Category ---")
revenue_by_category = df.groupBy("category") \
    .agg(round(sum(col("price") * col("quantity")), 2).alias("total_revenue")) \
    .orderBy(desc("total_revenue"))
revenue_by_category.show()

print("\n--- Monthly Sales Trend ---")
monthly_sales = df.withColumn("sales_month", month("order_date")) \
    .withColumn("sales_year", year("order_date")) \
    .groupBy("sales_year", "sales_month") \
    .agg(round(sum(col("price") * col("quantity")), 2).alias("total_sales")) \
    .orderBy("sales_year", "sales_month")
monthly_sales.show(30)

print("\n--- Return Rate by Category (%) ---")
return_rate = df.groupBy("category") \
    .agg(
        count("*").alias("total_orders"),
        sum("is_returned").alias("returned_orders")
    ) \
    .withColumn("return_rate", round((col("returned_orders") / col("total_orders")) * 100, 2)) \
    .orderBy(desc("return_rate"))
return_rate.show()

print("\n--- Top 5 Users by Total Spend ---")
top_users = df.groupBy("user_id") \
    .agg(round(sum(col("price") * col("quantity")), 2).alias("total_spend")) \
    .orderBy(desc("total_spend")) \
    .limit(5)
top_users.show()

print("\n--- Category with Highest Return Rate ---")
highest_return_category = return_rate.orderBy(desc("return_rate")).first()
if highest_return_category:
    print(f"Category: {highest_return_category['category']}")
    print(f"Return Rate: {highest_return_category['return_rate']}%")

print("\n✅ KPI calculation complete.")

print("\n📊 Generating visualization...")

monthly_sales_pd = monthly_sales.toPandas()

monthly_sales_pd['year_month'] = pd.to_datetime(monthly_sales_pd['sales_year'].astype(str) + '-' + monthly_sales_pd['sales_month'].astype(str))
monthly_sales_pd = monthly_sales_pd.sort_values('year_month')

plt.figure(figsize=(15, 6))
sns.lineplot(x='year_month', y='total_sales', data=monthly_sales_pd, marker='o', color='b')
plt.title('Monthly Sales Trend', fontsize=16)
plt.xlabel('Month')
plt.ylabel('Total Sales ($)')
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--')
plt.tight_layout()
plt.show()

print("✅ Visualization generated.")

spark.stop()
print("\n🎉 Project finished. SparkSession stopped.")
