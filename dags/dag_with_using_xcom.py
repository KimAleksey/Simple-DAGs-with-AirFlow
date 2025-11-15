from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from requests import get
from pandas import DataFrame
from os import path, getenv
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/config/.env")
API_KEY = getenv('API_KEY')
url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q=Moscow"

def extract(**context):
    print(url)
    request = get(url)
    context['ti'].xcom_push(key='weather_data_json', value=request.json())


def transform(**context):
    data = context['ti'].xcom_pull(key='weather_data_json', task_ids='extract')
    location_keys = ['name', 'region', 'country', 'tz_id', 'localtime']
    current_keys = ['temp_c', 'wind_kph', 'humidity', 'feelslike_c']
    data_to_data_frame = {}
    for key in data.keys():
        if key == 'location':
            for k, v in data[key].items():
                if k in location_keys:
                    data_to_data_frame[k] = v
        if key == 'current':
            for k, v in data[key].items():
                if k in current_keys:
                    data_to_data_frame[k] = v
    df = DataFrame([data_to_data_frame])
    context['ti'].xcom_push(key='data_to_data_frame', value=df)


def load(**context):
    data = context['ti'].xcom_pull(key='data_to_data_frame', task_ids='transform')
    file_path = "/opt/airflow/data/weather_data.csv"
    file_exists = path.exists(file_path)
    data.to_csv("/opt/airflow/data/weather_data.csv", mode="a", header=not file_exists, index=False, encoding='utf-8', sep=';')
    print("Inserted: \n", data)


default_args = {
    'owner': 'kim-av',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id='dag_with_xcom_for_weather_to_csv',
         description='DAG to try xcom',
         catchup=False,
         schedule_interval='@daily',
         default_args=default_args,
    ) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    extract >> transform >> load