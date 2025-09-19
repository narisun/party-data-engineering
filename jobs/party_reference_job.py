# jobs/party_reference_job.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# --- UPGRADE: Import Great Expectations V3 API components ---
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# --- Configuration ---
# BEST PRACTICE: Read credentials from environment variables, not hardcoded.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

BRONZE_BUCKET = "s3a://bronze"
SILVER_BUCKET = "s3a://silver"
GOLD_BUCKET = "s3a://gold"

# --- Spark Session Initialization ---
def get_spark_session():
    """Initializes and returns a Spark session configured for MinIO."""
    return (
        SparkSession.builder.appName("PartyReferenceETL")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

def run_job():
    spark = get_spark_session()
    print("Spark Session created successfully.")

    # --- Bronze Layer: Ingest raw data ---
    ny_df = spark.read.option("header", "true").csv(
        "/opt/airflow/data/ny_businesses.csv"
    )
    ca_df = spark.read.option("header", "true").csv(
        "/opt/airflow/data/ca_businesses.csv"
    )

    ny_df.write.format("parquet").mode("overwrite").save(
        f"{BRONZE_BUCKET}/ny_businesses"
    )
    ca_df.write.format("parquet").mode("overwrite").save(
        f"{BRONZE_BUCKET}/ca_businesses"
    )
    print("Successfully wrote raw data to Bronze layer.")

    # --- Silver Layer: Clean, Standardize, and Validate ---
    bronze_ny = spark.read.format("parquet").load(f"{BRONZE_BUCKET}/ny_businesses")
    bronze_ca = spark.read.format("parquet").load(f"{BRONZE_BUCKET}/ca_businesses")

    ny_standardized = bronze_ny.withColumn(
        "registration_date", to_date(col("registration_date"), "yyyy-MM-dd")
    )
    ca_standardized = (
        bronze_ca.withColumnRenamed("reg_id", "registration_id")
        .withColumnRenamed("company_name", "business_name")
        .withColumnRenamed("sector", "industry")
        .withColumnRenamed("reg_date", "registration_date")
        .withColumnRenamed("postal_code", "zip_code")
        .withColumnRenamed("region", "state")
        .withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))
    )

    combined_df = ny_standardized.unionByName(ca_standardized)
    print("Schemas standardized and data combined.")

    # --- UPGRADE: Data Quality Check with Great Expectations V3 API ---
    context = gx.get_context(project_root_dir="/opt/airflow/gx")
    validator = context.sources.pandas_default.read_spark(combined_df)
    expectation_suite = context.get_expectation_suite("silver_businesses_suite")
    
    # Create and run a checkpoint
    checkpoint = Checkpoint(
        name="silver_businesses_checkpoint",
        data_context=context,
        validator=validator,
        expectation_suite=expectation_suite,
    )
    results = checkpoint.run()

    if not results["success"]:
        print("Data quality checks failed!")
        print(results) # In production, you would log this to a monitoring system
        raise Exception("Silver data did not pass quality validation.")
    print("Data quality checks passed successfully.")
    # --- END UPGRADE ---

    (
        combined_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_BUCKET}/businesses")
    )
    print("Successfully wrote cleaned data to Silver layer (Delta format).")

    # --- Gold Layer: Create aggregated business view ---
    silver_businesses = spark.read.format("delta").load(f"{SILVER_BUCKET}/businesses")

    gold_df = silver_businesses.select(
        "registration_id", "business_name", "industry", "zip_code", "state"
    ).where(col("business_name").isNotNull())

    (
        gold_df.write.format("delta")
        .mode("overwrite")
        .save(f"{GOLD_BUCKET}/businesses_lookup")
    )
    print("Successfully wrote aggregated data to Gold layer (Delta format).")

    spark.stop()


if __name__ == "__main__":
    run_job()