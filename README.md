# de_project_usgs_earthquakes

## Project Overview
The core objective of this project is to create a reliable and scalable workflow for earthquake data. This involves:
- **Automated Data Ingestion**: Regularly fetching raw earthquake data from the USGS API.
- **Data Storage**: Storing both raw and processed data efficiently in cloud storage.
- **Data Transformation**: Structuring and cleaning the data into a usable format for analysis.
- **Data Warehousing**: Loading transformed data into a cloud data warehouse for querying and reporting.
- **Data Visualization**: Summarizing data and information into trends and charts using a business intelligence tool.

---
## 🏗️ Data Architecture
The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Data Architecture](docs/png/data_architecture.png)

1. **Bronze**: The Bronze layer serves as the raw data landing zone. It holds external tables in BigQuery that link directly to data in GCS buckets. Raw JSON files from the API are stored in a raw folder, and after conversion, optimized Parquet files are placed in a parquet folder.
2. **Silver**: This layer includes data cleansing, standardization, and normalization process to prepare data for analysis.
3. **Gold**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---

## 🧰 Tech Stack
- **USGS Earthquake Data API**: The primary data source, providing real-time and historical earthquake information. This is a REST API, allowing for programmatic access to the data.
- **Apache Airflow**: The central orchestration platform. Airflow schedules and manages the entire data pipeline, ensuring tasks run in the correct order, handling retries, and providing monitoring capabilities.
- **Google Cloud Storage (GCS)**: Used for data landing and staging. Raw JSON data and intermediate Parquet files are stored here before being loaded into BigQuery.
- **Google BigQuery**: A fully managed, serverless data warehouse. BigQuery stores the structured data, enabling fast analytical queries. It's organized into bronze, silver, and gold layers to reflect data maturity.
- **dbt (data build tool)**: Employed for data transformation and modeling within BigQuery. dbt allows for SQL-based transformations, version control of data models, and automated testing, ensuring data quality and consistency across the silver and gold layers.
- **Docker & Docker Compose**: Used to containerize the Airflow environment, providing a consistent and isolated development and deployment setup.
- **Microsoft Power BI**: A powerful business intelligence (BI) tool used for data visualization and interactive dashboards. It connects directly to the curated data in BigQuery's gold layer to provide insights and reporting.
- **Notion**: Utilized for project planning, task management, and documentation. It serves as a central hub for outlining project requirements, tracking progress, and organizing key information. ![Notion Project Steps](https://prickle-philosophy-032.notion.site/Data-Engineering-Project-2384b48d6676803fb04ae585b070cf8d?source=copy_link)

---

## Data Pipeline Flow
The pipeline follows a typical Extract, Load, Transform (ELT) pattern:

1. **Extract (E)**: Airflow triggers a Python script to download raw earthquake data in JSON format from the USGS API.

2. **Load (L)** - Staging: The raw JSON files are organized into a structured directory (e.g., by year) and then uploaded to a designated "raw" bucket in GCS.

3. **Transform (T)** - Intermediate: The raw JSON files are converted into Parquet format, which is more efficient for columnar storage and analytical querying, and then uploaded to a "parquet" bucket in GCS.

4. **Load (L)** - Data Lake/Warehouse: BigQuery datasets (bronze, silver, gold) are created. An external table is established in the bronze layer, pointing directly to the Parquet files in GCS, acting as a raw data lake layer.

5. **Transform (T)** - Modeling: dbt is used to build and test data models. It transforms the raw data from the bronze layer into cleaned, structured tables in the silver layer, and then aggregates/prepares data for analytical use cases in the gold layer.

This project demonstrates a robust, cloud-native approach to building a scalable and maintainable data pipeline for analytical purposes.

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.
