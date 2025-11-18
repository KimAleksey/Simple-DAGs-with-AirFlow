from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'kim-av',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'tags': ['example'],
    'start_date': datetime(2020, 1, 1),
}


def print_dag_start(**context):
    task_start = context["execution_date"]
    print("Task execution date: ", task_start)


with DAG(dag_id='dag_that_triggers_another_dag', catchup=False, schedule_interval='@daily', default_args=default_args) as dag:

    start_task = EmptyOperator(task_id='start_task')

    print_date_task = PythonOperator(task_id='print_date_task', python_callable=print_dag_start)

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='simple_test_dag',
        wait_for_completion=False,
        deferrable=False,
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> print_date_task >> trigger_dag_task >> end_task