from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.decorators import task

schedule_interval = timedelta(hours=1)

def simple_function():
    print("hello world")
    print(datetime.now())

with DAG(
    dag_id="simple_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=schedule_interval,
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id="say_hello_python",
        bash_command="echo 'Hello, Python!'"
    )

    t2 = BashOperator(
        task_id="say_hello_airflow",
        bash_command="echo 'Hello, Airflow!'"
    )

    t3 = BashOperator(
        task_id="say_hello_python_airflow",
        bash_command="echo 'Hello, Python, Airflow!'"
    )

    t4 = BashOperator(
        task_id="say_hello_everything",
        bash_command="echo 'Hello, Everything!'"
    )

    t5 = PythonOperator(
        task_id="print_date_time",
        python_callable=simple_function,
    )

    t1 >> t3
    t2 >> t3
    t3 >> [t4, t5]

with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag2:

    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    hello >> airflow()