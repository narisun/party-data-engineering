# jobs/party_reference_job.py
import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Optional: Great Expectations (Fluent API)
try:
    import great_expectations as gx
    from great_expectations.core import ExpectationSuite
    GE_AVAILABLE = True
except Exception:
    GE_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("party_reference_job")

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

BRONZE_BUCKET = "s3a://bronze"
SILVER_BUCKET = "s3a://silver"
GOLD_BUCKET   = "s3a://gold"

# --- Spark Session Initialization ---
def get_spark_session():
    """Initializes and returns a Spark session configured for MinIO + Delta."""
    spark = (
        SparkSession.builder.appName("PartyReferenceETL")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    return spark

# --- GX helpers (idempotent) ---
def _gx_get_or_add_spark_ds(context, name: str = "local_spark"):
    """
    Return existing Spark datasource if present; otherwise create it.
    Works across GX Fluent versions where get_datasource may raise different exceptions.
    """
    try:
        ds = context.get_datasource(name)  # GX >=0.16 Fluent API
        return ds
    except Exception:
        pass
    # Not found: add it
    return context.sources.add_spark(name=name)

def _gx_ensure_dataframe_asset(ds, asset_name: str):
    """
    Ensure a named DataFrame asset exists on the datasource and return it.
    """
    try:
        # In recent GX, assets acts like a dict-like mapping
        if hasattr(ds, "assets"):
            if isinstance(ds.assets, dict):
                if asset_name in ds.assets:
                    return ds.assets[asset_name]
            else:
                # Fallback: iterable of assets with .name
                for a in getattr(ds, "assets", []):
                    if getattr(a, "name", None) == asset_name:
                        return a
        # If we got here, add it
        return ds.add_dataframe_asset(name=asset_name)
    except Exception:
        # If anything odd with internals, just try to add (GX will error if true dup)
        return ds.add_dataframe_asset(name=asset_name)

def run_job():
    spark = get_spark_session()
    log.info("Spark Session created successfully.")

    # --- Bronze: Ingest raw CSV from local path into MinIO (Parquet) ---
    ny_df = spark.read.option("header", "true").csv("/opt/airflow/data/ny_businesses.csv")
    ca_df = spark.read.option("header", "true").csv("/opt/airflow/data/ca_businesses.csv")

    ny_df.write.format("parquet").mode("overwrite").save(f"{BRONZE_BUCKET}/ny_businesses")
    ca_df.write.format("parquet").mode("overwrite").save(f"{BRONZE_BUCKET}/ca_businesses")
    log.info("Wrote raw data to Bronze layer.")

    # --- Silver: Standardize schema ---
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
    log.info("Schemas standardized and data combined for Silver.")

    # --- Data Quality (Great Expectations) - optional gate ---
    if GE_AVAILABLE:
        try:
            context = gx.get_context(project_root_dir="/opt/airflow/gx")
            suite_name = "silver_businesses_suite"

            # ---- Suite get-or-create using public Fluent APIs ----
            try:
                suite = context.get_expectation_suite(suite_name)
            except Exception:
                suite = ExpectationSuite(expectation_suite_name=suite_name)
                context.add_or_update_expectation_suite(expectation_suite=suite)

            # Datasource / asset -> build batch request from Spark DataFrame (idempotent)
            ds = _gx_get_or_add_spark_ds(context, name="local_spark")
            asset = _gx_ensure_dataframe_asset(ds, asset_name="silver_businesses")
            batch_request = asset.build_batch_request(dataframe=combined_df)

            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name,
            )

            # Expectations
            validator.expect_table_row_count_to_be_between(min_value=1)
            validator.expect_column_values_to_not_be_null(column="registration_id")

            result = validator.validate()
            if not result.success:
                raise RuntimeError("Silver data did not pass quality validation.")

            # Persist any updates to the suite (supported API)
            try:
                context.add_or_update_expectation_suite(
                    expectation_suite=validator.get_expectation_suite()
                )
            except Exception:
                pass

            log.info("Great Expectations checks passed.")
        except Exception as ge_err:
            log.error("Great Expectations failed: %s", ge_err)
            # Fail the job if you want strict gating:
            raise
    else:
        log.warning("Great Expectations not installed/available; skipping data quality checks.")

    # --- Write Silver (Delta) ---
    (
        combined_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_BUCKET}/businesses")
    )
    log.info("Wrote cleaned data to Silver (Delta).")

    # --- Gold: Simple curated lookup view (Delta) ---
    silver_businesses = spark.read.format("delta").load(f"{SILVER_BUCKET}/businesses")

    gold_df = (
        silver_businesses
        .select("registration_id", "business_name", "industry", "zip_code", "state")
        .where(col("business_name").isNotNull())
    )

    (
        gold_df.write.format("delta")
        .mode("overwrite")
        .save(f"{GOLD_BUCKET}/businesses_lookup")
    )
    log.info("Wrote aggregated data to Gold (Delta).")

    spark.stop()
    log.info("Job completed successfully.")

if __name__ == "__main__":
    try:
        run_job()
    except Exception:
        log.exception("Job failed with an unhandled exception.")
        sys.exit(1)
