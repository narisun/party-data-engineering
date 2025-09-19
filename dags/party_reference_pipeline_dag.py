# dags/party_reference_pipeline_dag.py
import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- BEST PRACTICE: Use the @dag decorator for cleaner DAG definition ---
@dag(
    dag_id='party_reference_medallion_pipeline',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['medallion', 'spark', 'minio'],
)
def party_reference_pipeline():
    # --- UPGRADE: Updated package versions for Spark 3.5 ---
    # Delta Lake 3.2.0 is compatible with Spark 3.5
    # Hadoop-AWS 3.3.6 provides S3A filesystem support
    # GX 0.18.9 is the version used in the original project
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_party_reference_spark_job',
        application='/opt/airflow/jobs/party_reference_job.py',
        conn_id='spark_default',  # Ensure this is configured in Airflow UI: spark://spark-master:7077
        packages="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.6,com.great-expectations:great-expectations:0.18.9",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )

# Instantiate the DAG
party_reference_pipeline()