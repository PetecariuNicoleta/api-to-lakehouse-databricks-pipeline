# API-to-Lakehouse-Databricks-Pipeline

This project demonstrates an end-to-end data engineering pipeline built using Databricks and Delta Lake, following the Medallion Architecture (Bronze, Silver, Gold layers).
The pipeline ingests data from a public API, processes it through multiple transformation layers, and produces aggregated datasets ready for analytics.

## Architecture
API → Bronze Layer → Silver Layer → Gold Layer

1. Bronze: Raw data ingestion from API
2. Silver: Data cleaning, deduplication, validation
3. Gold: Aggregated business-level data

## Technologies Used

* Python
* PySpark
* Databricks
* Delta Lake

## Pipeline Steps
🟫 Bronze Layer (Ingestion)
* Data is fetched from a REST API
* Stored as raw data in Delta format
* Metadata (ingestion timestamp) is added

🥈 Silver Layer (Transformation)

* Data cleaning and validation
* Duplicate removal using window functions
* Schema standardization

🥇 Gold Layer (Aggregation)

* Business-level aggregations
* Example: total records per user

🔁 Key Features

* Incremental data processing
* Idempotent pipeline design
* Deduplication logic using window functions
* Layered architecture (Bronze, Silver, Gold)
* Orchestration using Databricks Jobs



![Screenshot 2026-03-26 at 12.19.45_1774520422740.png](https://github.com/PetecariuNicoleta/api-to-lakehouse-databricks-pipeline.git/Screenshot 2026-03-26 at 12.19.45_1774520422740.png)

📊 Future Improvements

* Implement incremental loads using MERGE
* Handle late-arriving data
* Add data quality checks framework
* Introduce streaming ingestion
* Partitioning and performance optimization

💡 What I Learned

* Designing scalable data pipelines
* Working with Delta Lake and medallion architecture
* Handling duplicates and ensuring data quality
* Implementing end-to-end ETL workflows