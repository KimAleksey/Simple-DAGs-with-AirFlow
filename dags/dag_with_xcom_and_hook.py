from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from pendulum import datetime


@task.python
def get_ip_from_api():
    hook = HttpHook(method='GET', http_conn_id='ipify_api')
    response = hook.run().json()
    return response


@task.python
def printing_current_ip(response):
    print(response)
    print(response['ip'])


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'tags': ['airflow']
}


with DAG(dag_id='dag_with_xcom_and_hook', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:
    response = get_ip_from_api()
    printing_current_ip(response)