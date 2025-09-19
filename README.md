# Party Data Engineering Project

This project demonstrates a modern data engineering pipeline using a Medallion architecture (Bronze, Silver, Gold layers). It ingests raw business registration data, cleans and validates it, and creates an aggregated, queryable dataset.

The entire stack is containerized using Docker and orchestrated with Airflow.

---

### 🏛️ Architecture

* **Orchestration**: **Apache Airflow** schedules and triggers the data pipeline.
* **Data Lake Storage**: **MinIO** acts as an S3-compatible object storage for our Bronze, Silver, and Gold data layers.
* **Data Processing**: **Apache Spark** runs the ETL/ELT logic to transform data between layers.
* **Data Format**: **Delta Lake** is used for the Silver and Gold layers to provide ACID transactions, time travel, and reliability to the data lake.
* **Data Quality**: **Great Expectations** is integrated into the Spark job to validate data as it moves to the Silver layer.
* **Query Engine**: **Trino** provides a high-performance, distributed SQL query engine to analyze the final data in the Gold layer.
* **Data Visualization**: **Apache Superset** connects to Trino to build dashboards and visualize the business data.


---

### 🚀 Getting Started

#### Prerequisites

* Docker and Docker Compose
* A `.env` file (copy from `.env.example` and update if necessary)

#### 1. Build and Run the Services

From the root of the project, run the following command:

```bash
docker-compose up -d --build