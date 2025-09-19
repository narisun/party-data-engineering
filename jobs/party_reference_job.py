# jobs/party_reference_job.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from great_expectations.dataset import SparkDFDataset

# --- Configuration ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BRONZE_BUCKET = "s3a://bronze"
SILVER_BUCKET = "s3a://silver"
GOLD_BUCKET = "s3a://gold"

# --- Spark Session Initialization ---
def get_spark_session():
    return (
        SparkSession.builder.appName("PartyReferenceETL")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def run_job():
    spark = get_spark_session()
    print("Spark Session created successfully.")

    # --- Bronze Layer: Ingest raw data ---
    # In a real scenario, this would be done by the Airflow DAG copying files
    # For this example, we read directly from the mounted 'data' directory
    ny_df = spark.read.option("header", "true").csv("/opt/airflow/data/ny_businesses.csv")
    ca_df = spark.read.option("header", "true").csv("/opt/airflow/data/ca_businesses.csv")

    ny_df.write.format("parquet").mode("overwrite").save(f"{BRONZE_BUCKET}/ny_businesses")
    ca_df.write.format("parquet").mode("overwrite").save(f"{BRONZE_BUCKET}/ca_businesses")
    print("Successfully wrote raw data to Bronze layer.")

    # --- Silver Layer: Clean, Standardize, and Validate ---
    # 1. Read from Bronze
    bronze_ny = spark.read.format("parquet").load(f"{BRONZE_BUCKET}/ny_businesses")
    bronze_ca = spark.read.format("parquet").load(f"{BRONZE_BUCKET}/ca_businesses")

    # 2. Standardize Schemas
    ny_standardized = bronze_ny.withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))

    ca_standardized = (
        bronze_ca.withColumnRenamed("reg_id", "registration_id")
        .withColumnRenamed("company_name", "business_name")
        .withColumnRenamed("sector", "industry")
        .withColumnRenamed("reg_date", "registration_date")
        .withColumnRenamed("postal_code", "zip_code")
        .withColumnRenamed("region", "state")
        .withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))
    )

    # 3. Union standardized data
    combined_df = ny_standardized.unionByName(ca_standardized)
    print("Schemas standardized and data combined.")

    # 4. Data Quality Check with Great Expectations
    ge_df = SparkDFDataset(combined_df)
    expectation_suite = "/opt/airflow/gx/expectations/silver_businesses_suite.json"
    results = ge_df.validate(expectation_suite_path=expectation_suite)
    
    if not results["success"]:
        print("Data quality checks failed!")
        print(results)
        raise Exception("Silver data did not pass quality validation.")
    print("Data quality checks passed successfully.")

    # 5. Write to Silver as Delta table
    (combined_df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .save(f"{SILVER_BUCKET}/businesses"))
    print("Successfully wrote cleaned data to Silver layer (Delta format).")

    # --- Gold Layer: Create aggregated business view ---
    # 1. Read from Silver
    silver_businesses = spark.read.format("delta").load(f"{SILVER_BUCKET}/businesses")

    # 2. Create final view (simple select for this example)
    # A real use case might involve de-duplication, creating master keys, etc.
    gold_df = silver_businesses.select(
        "registration_id", "business_name", "industry", "zip_code", "state"
    ).where(col("business_name").isNotNull())
    
    # 3. Write to Gold as Delta table
    (gold_df.write.format("delta")
     .mode("overwrite")
     .save(f"{GOLD_BUCKET}/businesses_lookup"))
    print("Successfully wrote aggregated data to Gold layer (Delta format).")

    spark.stop()

if __name__ == "__main__":
    run_job()