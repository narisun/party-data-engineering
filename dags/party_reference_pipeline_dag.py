# dags/party_reference_pipeline_dag.py
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

@dag(
    dag_id='party_reference_medallion_pipeline',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['medallion', 'spark', 'minio'],
)
def party_reference_pipeline():
    SparkSubmitOperator(
    task_id='submit_party_reference_spark_job',
    conn_id='spark_default',
    application='/opt/airflow/jobs/party_reference_job.py',
    # py_files=None,  # remove the GE / ruamel wheels here
    conf={
        "spark.pyspark.driver.python": "python3",
        "spark.pyspark.python": "python3",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": "minioadmin",
        "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    },
    packages="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4",
    verbose=True,
)


party_reference_pipeline()
