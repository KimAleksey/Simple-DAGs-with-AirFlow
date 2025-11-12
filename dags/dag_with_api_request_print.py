from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from requests import get


def get_data_from_api():
    response = get(url="https://official-joke-api.appspot.com/jokes/random").json()
    print(response['setup'] + "...\n" + response['punchline'])


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': '@daily'
}

with DAG(dag_id='get_data_from_api', catchup=False, default_args=default_args) as dag:

    get_data = PythonOperator(task_id='get_data_from_api', python_callable=get_data_from_api)

    get_data