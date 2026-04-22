# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Data Engineering Pipeline: Bakery Sales Analysis
# MAGIC ### Architecture: Medallion (Bronze -> Silver -> Gold)
# MAGIC **Author:** Raydesel Ariel  
# MAGIC **Tools:** Databricks, PySpark, Delta Lake, Unity Catalog  
# MAGIC
# MAGIC This project demonstrates a complete ETL pipeline using simulated bakery sales data. The goal is to transform raw transactional data into actionable business insights regarding city performance and customer behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Environment Setup & Data Discovery
# MAGIC In this step, we initialize our project schema within the Unity Catalog. We are utilizing the `samples.bakehouse` dataset, which contains:
# MAGIC * `sales_transactions`: Granular sales records.
# MAGIC * `sales_customers`: Demographic and loyalty information.
# MAGIC * `sales_franchises`: Regional location data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS raydesel_bakery_project;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bronze Layer - Raw Data Ingestion
# MAGIC **Goal:** Create a 1:1 copy of the source data in **Delta format**.
# MAGIC
# MAGIC In the Bronze layer, we maintain the raw state of the data. This ensures we have a permanent, auditable record for troubleshooting. We use Delta Lake to enable ACID transactions and schema enforcement.

# COMMAND ----------

tables_to_import = ["sales_transactions", "sales_customers", "sales_franchises"]

for table_name in tables_to_import:
    df = spark.read.table(f"samples.bakehouse.{table_name}")
    
    target_table = f"raydesel_bakery_project.bronze_{table_name}"
    
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)
    
    print(f"Success: {target_table} has been created.")

display(spark.table("raydesel_bakery_project.bronze_sales_transactions").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Silver Layer - Data Cleansing & Transformation
# MAGIC **Goal:** Create a "Single Version of Truth."
# MAGIC
# MAGIC In this stage, we perform the "Engineering" heavy lifting:
# MAGIC 1. **Joining:** Combining transactions with customer data for a denormalized view.
# MAGIC 2. **Filtering:** Removing "garbage" data (records with 0 or negative quantities).
# MAGIC 3. **Type Casting:** Converting raw strings/timestamps into a clean `sale_date` format.
# MAGIC 4. **Data Integrity:** Handling null values to ensure downstream accuracy.

# COMMAND ----------

from pyspark.sql.functions import col, to_date

transactions_df = spark.table("raydesel_bakery_project.bronze_sales_transactions")
customers_df = spark.table("raydesel_bakery_project.bronze_sales_customers")

silver_df = transactions_df.join(
    customers_df, 
    transactions_df.customerID == customers_df.customerID, 
    "inner"
).select(
    transactions_df["transactionID"],
    transactions_df["customerID"],
    customers_df["first_name"],
    customers_df["last_name"],
    customers_df["city"],
    transactions_df["quantity"],
    transactions_df["dateTime"]
)

silver_clean_df = silver_df.filter(col("quantity") > 0) \
    .withColumn("sale_date", to_date(col("dateTime"))) \
    .drop("dateTime")

silver_clean_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("raydesel_bakery_project.silver_sales_cleaned")

print("Silver Layer table 'silver_sales_cleaned' created successfully!")
display(spark.table("raydesel_bakery_project.silver_sales_cleaned").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Gold Layer - Aggregated Business Metrics
# MAGIC **Goal:** Deliver high-level insights for decision-makers.
# MAGIC
# MAGIC The Gold layer contains highly optimized, aggregated data. Here, we calculate **Total Sales Volume per City**. This table is ready to be consumed by BI tools (like Power BI or Tableau) or used to build the final executive dashboard within Databricks.

# COMMAND ----------

from pyspark.sql.functions import sum, desc

silver_df = spark.table("raydesel_bakery_project.silver_sales_cleaned")

gold_city_performance_df = silver_df.groupBy("city") \
    .agg(sum("quantity").alias("total_cookies_sold")) \
    .orderBy(desc("total_cookies_sold"))

gold_city_performance_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("raydesel_bakery_project.gold_city_performance")

display(spark.table("raydesel_bakery_project.gold_city_performance"))