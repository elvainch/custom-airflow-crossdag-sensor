from airflow import DAG
from dagstatussensor import DagStatusSensor

from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

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
    dag_id='external_sensor_testing',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval="*/2 * * * *",
    start_date=days_ago(1),
    tags=['example'],
    catchup=False,
    max_active_runs=1
)

def what_status(**kwargs):
    return kwargs['ti'].xcom_pull(dag_id="external_sensor_testing", key='status')

t1 = DagStatusSensor(
    task_id='task_sensor',
    status_to_check ='success',
    dag_name="primary_dag",
    dag=dag,
    run_to_check=0,
    timeout=40,
    do_xcom_push=True,
    poke_interval=20,
    soft_fail=False
)

t2 = BranchPythonOperator(
    task_id='branching',
    python_callable=what_status,
    provide_context=True,
    trigger_rule='all_done',
    dag=dag)

op = DummyOperator(task_id='successful', dag=dag,trigger_rule='all_done')
op2 = DummyOperator(task_id='failure', dag=dag,trigger_rule='all_done')

t1 >> t2
t2 >> op
t2 >> op2