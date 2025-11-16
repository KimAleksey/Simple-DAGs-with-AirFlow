from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import now, duration
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from datetime import timedelta, datetime

def print_date():
    print("DAG working_with_pg_db has been completed. Starting task.")
    print(now())


default_args = {
    'owner': 'kim-av',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='dag_with_dependence',
         catchup=False,
         default_args=default_args,
         schedule=timedelta(days=1),
         max_active_runs=1
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    wait_for_extract = ExternalTaskSensor(
        task_id="wait_for_extract",
        external_dag_id="working_with_pg_db",
        external_task_id="delete_old_files_from_data_folder", # None - Ждать выполнения всего DAG
        allowed_states=['success'],
        failed_states=['failed'],
    )

    print_after_sql_load = PythonOperator(
        task_id='print_after_sql_load',
        python_callable=print_date,
    )

    start >> wait_for_extract >> print_after_sql_load >> end