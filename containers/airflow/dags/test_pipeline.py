from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the default parameters
default_args = {
    'owner': 'eclavel',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    '01_test_connection',
    default_args=default_args,
    description='A simple test DAG to verify the execution engine',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 6, 29), # Set to yesterday so it is immediately ready
    catchup=False,
    tags=['testing'],
) as dag:

    # Define a simple task
    task_hello_world = BashOperator(
        task_id='print_hello',
        bash_command='echo "Airflow execution engine is operational!"',
    )

    task_hello_world