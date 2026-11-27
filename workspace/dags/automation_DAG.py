'''
=================================================
Apache Airflow DAG Orchestration

Author : Ryan Farid
Batch : CODA

This script configures the automated workflow orchestration schedule for the ETL pipeline (Extract >> Transform >> Load) using Apache Airflow.

Schedule Interval: Every Saturday at 09:10 AM, 09:20 AM, and 09:30 AM starting November 1, 2024.
=================================================
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import extract_data
from transform import transform_data
from load import load_data

RAW_DATA_PATH = "/opt/airflow/dags/raw_dataset.csv"
MONGO_URI = os.getenv("MONGODB_URI")
DB_NAME = "portfolio3-db"
COLLECTION_NAME = "palm_oil_supply_chain"

TEMP_RAW_PATH = "/tmp/df_raw.parquet"
TEMP_CLEAN_PATH = "/tmp/df_clean.parquet"

def run_extract_task(**kwargs) -> str:
    '''Airflow task wrapper to trigger data extraction.'''
    df_raw = extract_data(RAW_DATA_PATH)
    df_raw.write.mode("overwrite").parquet(TEMP_RAW_PATH)
    return TEMP_RAW_PATH

def run_transform_task(**kwargs) -> str:
    '''Airflow task wrapper to trigger data transformation.'''
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    df_raw = spark.read.parquet(TEMP_RAW_PATH)
    df_clean = transform_data(df_raw)
    df_clean.write.mode("overwrite").parquet(TEMP_CLEAN_PATH)
    return TEMP_CLEAN_PATH

def run_load_task(**kwargs) -> None:
    '''Airflow task wrapper to trigger MongoDB loading.'''
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
        
    df_clean = spark.read.parquet(TEMP_CLEAN_PATH)
    load_data(
        df=df_clean,
        mongo_uri=MONGO_URI,
        db_name=DB_NAME,
        collection_name=COLLECTION_NAME
        )
        
default_args = {
    'owner': 'Your Name',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

with DAG(
    dag_id='automation_DAG',
    default_args=default_args,
    description='Automated Palm Oil Supply Chain ETL Pipeline (PySpark & MongoDB)',
    schedule='10,20,30 9 * * 6',            # <-- Change
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=run_extract_task
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=run_transform_task
    )
    
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=run_load_task
    )

extract_task >> transform_task >> load_task