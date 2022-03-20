from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
dag = DAG(
    dag_id='primary_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(minutes=2),
    start_date=days_ago(1),
    tags=['example'],
    catchup=False,
    max_active_runs=1
)


t1 = BashOperator(
    task_id='before_sleep',
    depends_on_past=False,
    bash_command='echo "Running before sleep"',
    retries=3,
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag,
)

t3 = BashOperator(
    task_id='after_sleep',
    depends_on_past=False,
    bash_command='echo "Its running after sleep"',
    retries=3,
    dag=dag,
)

t1 >> t2 >> t3