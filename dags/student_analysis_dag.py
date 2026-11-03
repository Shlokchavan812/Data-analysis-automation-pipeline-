"""
Airflow DAG: Student Performance Data Analysis Pipeline
Schedule: Runs daily at 12:00 PM ('0 12 * * *')
"""

from datetime import datetime, timedelta
import os
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Resolve project root dynamically (works locally, in WSL, or in Docker)
ENV_PROJECT_DIR = os.getenv("AIRFLOW_PROJECT_DIR")
if ENV_PROJECT_DIR and os.path.exists(ENV_PROJECT_DIR):
    PROJECT_ROOT = ENV_PROJECT_DIR
elif os.path.exists("/opt/airflow/project"):
    PROJECT_ROOT = "/opt/airflow/project"
else:
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline import clean_data, transform_data, analyse_and_export

# Set timezone (e.g., local time or UTC). 
# Adjust 'Asia/Kolkata' or your preferred timezone if needed.
local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def verify_input_data_exists(**context):
    """Ensure the source dataset exists before running downstream steps."""
    input_file = os.path.join(PROJECT_ROOT, "student_performance_clean_reference (1).csv")
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Source file not found at: {input_file}")
    print(f"Verified source dataset exists: {input_file}")


def run_clean_step(**context):
    """Airflow task wrapper for data cleaning."""
    print("Executing Data Cleaning Task...")
    input_file = os.path.join(PROJECT_ROOT, "student_performance_clean_reference (1).csv")
    output_file = os.path.join(PROJECT_ROOT, "clean_students.csv")
    clean_data(input_path=input_file, output_path=output_file)
    print("Data Cleaning completed successfully.")


def run_transform_step(**context):
    """Airflow task wrapper for feature transformations."""
    print("Executing Data Transformation Task...")
    input_file = os.path.join(PROJECT_ROOT, "clean_students.csv")
    output_file = os.path.join(PROJECT_ROOT, "transformed_students.csv")
    transform_data(input_path=input_file, output_path=output_file)
    print("Data Transformation completed successfully.")


def run_analysis_step(**context):
    """Airflow task wrapper for summary calculation and reporting."""
    print("Executing Analysis and Reporting Task...")
    input_file = os.path.join(PROJECT_ROOT, "transformed_students.csv")
    summary_path = os.path.join(PROJECT_ROOT, "analysis_summary.csv")
    corr_path = os.path.join(PROJECT_ROOT, "analysis_correlation.csv")
    summary, _ = analyse_and_export(
        input_path=input_file,
        summary_path=summary_path,
        corr_path=corr_path
    )
    print("Analysis completed successfully. Summary generated:")
    print(summary)


with DAG(
    dag_id="student_data_analysis_pipeline",
    default_args=default_args,
    description="Automated Student Performance Data Analysis Pipeline running daily at 12 PM",
    # Cron '0 12 * * *' fires at 12:00 PM every day
    schedule="0 12 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["data_analysis", "student_performance", "daily_12pm"],
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    check_data = PythonOperator(
        task_id="check_source_data",
        python_callable=verify_input_data_exists,
    )

    clean_step = PythonOperator(
        task_id="clean_data",
        python_callable=run_clean_step,
    )

    transform_step = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform_step,
    )

    analyze_step = PythonOperator(
        task_id="analyze_and_export",
        python_callable=run_analysis_step,
    )

    end_pipeline = EmptyOperator(
        task_id="pipeline_completed"
    )

    # Task dependencies
    start_pipeline >> check_data >> clean_step >> transform_step >> analyze_step >> end_pipeline
