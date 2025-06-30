"""
This DAG schedules and runs the `serving.py` script, which pushes new data to Power BI.

Note:
    1. Place this file in your Airflow 'dags' folder.
    2. If you're using an Airflow version earlier than 2.6, replace `schedule` with `schedule_interval` in the DAG definition.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import os
import subprocess

# Load environment variables from .env file
load_dotenv(find_dotenv())

def push_data():
    """
    Executes the Hadoop/serving.py script as a subprocess.

    Raises:
        EnvironmentError: If the PROJECT_ROOT environment variable is not set.
        FileNotFoundError: If the serving.py script does not exist at the specified path.
        Exception: If the serving.py script fails to execute successfully.
    """
    project_path = os.getenv('PROJECT_ROOT')
    if not project_path:
        raise EnvironmentError("PROJECT_ROOT environment variable is not set.")
    
    serving_path = os.path.join(project_path, 'Hadoop', 'serving.py')
    if not os.path.exists(serving_path):
        raise FileNotFoundError(f"{serving_path} not found.")

    result = subprocess.run(
        ["python3", serving_path],
        capture_output=True,
        text=True
    )
    
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise Exception(f"Script failed with code {result.returncode}: {result.stderr}")

# Define the DAG
with DAG(
    dag_id="push_data_to_powerbi",
    start_date=datetime(2025, 6, 10),
    schedule="@daily",  # Replace with `schedule_interval` if using Airflow < 2.6
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='run_serving_job',
        python_callable=push_data
    )
