"""
Production-grade Apache Airflow DAG for orchestrating sales ETL pipeline.
Implements best practices: input file sensing, ETL, validation, reporting, alerting.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

PROJECT_ID = 'salem-463409'
REGION = 'us-central1'
BUCKET_NAME = f'{PROJECT_ID}-etl-source-data'
DATASET_ID = 'sales_analytics'
TABLE_ID = 'sales_data'
INPUT_FILE = 'raw-data/sample_sales.csv'

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='Production-grade Sales data ETL with Dataflow and BigQuery',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    tags=['etl', 'sales', 'dataflow', 'bigquery'],
) as dag:

    # Task 1: Wait for source data in GCS
    wait_for_source = GCSObjectExistenceSensor(
        task_id='wait_for_source_data',
        bucket=BUCKET_NAME,
        object=INPUT_FILE,
        poke_interval=60,
        timeout=60 * 60,
    )

    # Task 2: Run Dataflow ETL
    run_dataflow = DataflowStartFlexTemplateOperator(
        task_id='run_dataflow_etl',
        project_id=PROJECT_ID,
        location=REGION,
        body={
            'launchParameter': {
                'jobName': 'sales-etl-{{ ds_nodash }}',
                'parameters': {
                    'input': f'gs://{BUCKET_NAME}/{INPUT_FILE}',
                    'output': f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}',
                    'project': PROJECT_ID,
                },
                'environment': {
                    'tempLocation': f'gs://{BUCKET_NAME}/temp',
                    'stagingLocation': f'gs://{BUCKET_NAME}/staging',
                },
            }
        },
    )

    # Task 3: Validate loaded data in BigQuery
    validate_data = BigQueryCheckOperator(
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
        use_legacy_sql=False,
    )

    # Task 4: Generate summary report (could be an export or analytic query)
    summary_report = BigQueryInsertJobOperator(
        task_id='generate_summary_report',
        configuration={
            "query": {
                "query": f"""
                    SELECT 
                        COUNT(*) as total_sales,
                        ROUND(SUM(total_sale), 2) as revenue,
                        ROUND(AVG(total_sale), 2) as avg_sale,
                        COUNT(DISTINCT product) as unique_products,
                        MAX(sale_date) as latest_sale_date
                    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    # Task 5: Notify on failure (alerting)
    notify_failure = EmailOperator(
        task_id='notify_on_failure',
        to='data-team@yourcompany.com',
        subject='[ALERT] Sales ETL Pipeline Failure',
        html_content='The sales ETL pipeline failed. Please check the Airflow logs.',
        trigger_rule='one_failed',
    )

    # DAG structure
    wait_for_source >> run_dataflow >> validate_data >> summary_report
    [wait_for_source, run_dataflow, validate_data, summary_report] >> notify_failure
