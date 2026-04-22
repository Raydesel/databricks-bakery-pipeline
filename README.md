# 🍪 Bakery Sales: End-to-End Databricks Pipeline

![Dashboard Screenshot](images/1-location-performance.png)
![Dashboard Screenshot](images/2-sales-trends.png)
![Dashboard Screenshot](images/3-products-details.png)

## 📌 Project Overview
This project demonstrates a production-grade **Medallion Architecture** (Bronze → Silver → Gold) pipeline using **Databricks**, **PySpark**, and **Delta Lake**. I transformed raw, simulated bakery transaction data into an interactive executive dashboard that identifies regional sales performance and product trends.

## 🏗️ Data Architecture
The pipeline follows industry best practices to ensure data reliability and scalability:

* **Bronze Layer (Raw):** Ingested raw `sales_transactions`, `sales_customers`, and `sales_franchises` data from Unity Catalog samples into managed Delta tables.
* **Silver Layer (Cleansed):** Performed data engineering "heavy lifting" by joining transactions with customer demographics, filtering out test records (quantity <= 0), and casting raw timestamps into optimized Date formats.
* **Gold Layer (Curated):** Developed business-level aggregation tables. While the dashboard queries optimized Silver/Bronze views for maximum granularity, a Gold layer was established for high-performance regional ranking.

## 🛠️ Tech Stack & Key Concepts
* **Engine:** Apache Spark (PySpark) for distributed data processing.
* **Storage:** Delta Lake format to provide ACID transactions and "Time Travel" capabilities.
* **Governance:** Unity Catalog for centralized metadata management and the three-tier namespace (`catalog.schema.table`).
* **Visualization:** Databricks SQL Warehouse to power the executive dashboard.

## 📊 Executive Dashboard
The final output is a portable **Databricks SQL Dashboard** (`.lvdash.json`) providing real-time visibility into:
* **High-Level KPIs:** Total Revenue, Transaction Volume, and Average Order Value.
* **Geographic Insights:** Revenue distribution across different franchise cities.
* **Product Analytics:** Distribution of sales by cookie type and payment method.
* **Temporal Trends:** Daily revenue tracking to identify growth patterns.

## 📁 Repository Structure
* `notebooks/`: Contains the PySpark ETL logic exported as a `.py` source file.
* `dashboards/`: Contains the `Bakery_Executive_Insights.lvdash.json` file for portable dashboard deployment.
* `images/`: High-resolution screenshots of the architecture and final dashboard visualizations.

## 🚀 How to Use
1.  **Import Pipeline:** Upload the `.py` script from the `notebooks/` folder into your Databricks Workspace.
2.  **Run ETL:** Execute the notebook to build the `raydesel_bakery_project` schema and populate the tables.
3.  **Import Dashboard:** Navigate to **Dashboards** > **Create** > **Import dashboard from file** and select the `.lvdash.json` file to instantly recreate the visualizations.
