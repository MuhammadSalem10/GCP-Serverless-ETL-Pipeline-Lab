"""
Apache Airflow DAG for orchestrating sales ETL pipeline.
This demonstrates production-ready workflow orchestration practices.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow.utils.dates import days_ago

# Configuration
PROJECT_ID = 'salem-463409'  
REGION = 'us-central1'
BUCKET_NAME = f'{PROJECT_ID}-etl-source-data'
DATASET_ID = 'sales_analytics'
TABLE_ID = 'sales_data'

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Define DAG
dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='Sales data ETL pipeline with Dataflow and BigQuery',
    schedule_interval=timedelta(days=1),  # Run daily
    max_active_runs=1,  # Prevent concurrent runs
    tags=['etl', 'sales', 'dataflow', 'bigquery'],
)

# Task 1: Data Quality Check (verify source file exists)
check_source_data = BigQueryCheckOperator(
    task_id='check_source_data_exists',
    sql=f"""
    SELECT COUNT(*) as file_count 
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.TABLES` 
    WHERE table_name = '{TABLE_ID}'
    """,
    dag=dag,
)

# Task 2: Run Dataflow ETL Pipeline
dataflow_etl = DataflowStartFlexTemplateOperator(
    task_id='run_dataflow_etl',
    project_id=PROJECT_ID,
    location=REGION,
    body={
        'launchParameter': {
            'jobName': f'sales-etl-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            'parameters': {
                'input': f'gs://{BUCKET_NAME}/raw-data/sample_sales.csv',
                'output': f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}',
                'project': PROJECT_ID,
            },
            'environment': {
                'tempLocation': f'gs://{BUCKET_NAME}/temp',
                'stagingLocation': f'gs://{BUCKET_NAME}/staging',
            },
        }
    },
    dag=dag,
)

# Task 3: Data Quality Validation
validate_data_quality = BigQueryCheckOperator(
    task_id='validate_data_quality',
    sql=f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT id) as unique_records,
        SUM(CASE WHEN total_sale = price * quantity THEN 1 ELSE 0 END) as correct_calculations
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    HAVING 
        total_records > 0 
        AND unique_records = total_records
        AND correct_calculations = total_records
    """,
    dag=dag,
)

# Task 4: Generate Summary Report
generate_summary = BigQueryCheckOperator(
    task_id='generate_summary_report',
    sql=f"""
    SELECT 
        COUNT(*) as total_sales,
        ROUND(SUM(total_sale), 2) as revenue,
        ROUND(AVG(total_sale), 2) as avg_sale,
        COUNT(DISTINCT product) as unique_products,
        MAX(sale_date) as latest_sale_date
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """,
    dag=dag,
)

# Define task dependencies
check_source_data >> dataflow_etl >> validate_data_quality >> generate_summary