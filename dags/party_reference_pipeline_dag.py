# dags/party_reference_pipeline_dag.py
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='party_reference_medallion_pipeline',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['medallion', 'spark', 'minio'],
) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_party_reference_spark_job',
        application='/opt/airflow/jobs/party_reference_job.py',
        conn_id='spark_default',  # This uses spark://spark-master:7077 from Airflow config
        packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.great-expectations:great-expectations-spark_2.12:0.18.9",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )