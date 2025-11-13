from airflow import DAG
from airflow.models import TaskInstance, DagRun
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def get_dag_context(**kwargs):
    ti: TaskInstance = kwargs["task_instance"]
    print(f"Task id {ti.task_id}")
    print(f"Task run id {ti.run_id}")
    print(f"Task duration {ti.duration}")

    dr: DagRun = kwargs["dag_run"]
    print(f"Dag queued at {dr.queued_at}")
    print(f"Dag started at {dr.start_date}")
    print(f"Dag hash {dr.dag_hash}")


@task()
def print_dag_ending():
    print("Dag end")


default_args = {
    'owner': 'kim-av',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_with_context_printing',
    description='DAG with context printing',
    catchup=False,
    default_args=default_args,
    start_date=datetime(2020, 1, 1),
    schedule_interval=timedelta(days=1),
    tags=['example'],
) as dag:

    task1 = PythonOperator(
        task_id='task1', python_callable=get_dag_context
    )

    task2 = BashOperator(
        task_id='task2', bash_command='echo "After python function"'
    )

    task3 = BashOperator(
        task_id='task3', bash_command='date'
    )

    task1 >> [task2, task3] >> print_dag_ending()
