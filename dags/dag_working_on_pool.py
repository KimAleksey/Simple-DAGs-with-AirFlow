from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from time import time, sleep
from airflow.operators.python import PythonOperator


def python_func(**context):
    start = time()
    print("Logical date : ", context["dag_run"].logical_date, sep='')
    print("Date interval start: ", context["dag_run"].data_interval_start, sep='')
    print("Date interval end: ", context["dag_run"].data_interval_end, sep='')
    print("Start date: ", context["dag_run"].start_date, sep='')
    print("End date: ", context["dag_run"].end_date, sep='')
    print("Содержимое контекста: ", context.items(), sep='')
    end = time()
    print("Time evaluating: ", end-start, sep='')
    sleep(15)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 1),
}

with DAG(dag_id='dag_with_pool', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:

    start_task = EmptyOperator(task_id='start_task')

    task1 = PythonOperator(
        task_id='task_01',
        python_callable=python_func,
        pool="pool_for_3_jobs"
    )

    task2 = PythonOperator(
        task_id='task_02',
        python_callable=python_func,
        pool="pool_for_3_jobs"
    )

    task3 = PythonOperator(
        task_id='task_03',
        python_callable=python_func,
        pool="pool_for_3_jobs"
    )

    task4 = PythonOperator(
        task_id='task_04',
        python_callable=python_func,
        pool="pool_for_3_jobs"
    )

    task5 = PythonOperator(
        task_id='task_05',
        python_callable=python_func,
        pool="pool_for_3_jobs"
    )

    end_task = EmptyOperator(task_id='end_task')

    start_task >> [task1, task2, task3, task4, task5] >> end_task