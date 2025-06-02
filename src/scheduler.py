from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess, os

def run_serving_job():
    serving_path = os.path.join(os.getenv('HOME'), 'ODAP/src/serving.py')
    result = subprocess.run(
        ["python3", serving_path],
        capture_output=True,
        text=True
    )
    
    print(">>> STDOUT:")
    print(result.stdout)
    print(">>> STDERR:")
    print(result.stderr)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 31),
    'retries': 1,
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval='@daily',
)

task_run_serving_job = PythonOperator(
    task_id='run_serving_job',
    python_callable=run_serving_job,
    dag=dag,
)