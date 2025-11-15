from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from pendulum import datetime
from airflow.operators.empty import EmptyOperator

# Так как контейнер с Postgres был в отдельной сети, пришлось добавлять БД к сети
# docker network connect project1simpledag_default my-postgre-v1
# При перезапуске контейнера с AirFlow команду нужно будет выполнять повторно, так как эта настройка слетит


def create_table_in_demo_db():
    postgres_hook = PostgresHook(postgres_conn_id='data_base_pg_demo')
    result = postgres_hook.run("""
    CREATE TABLE IF NOT EXISTS bookings.flights_airflow (
    	flight_id int4 NOT NULL,
        route_no text NOT NULL,
        status text NOT NULL,
        scheduled_departure timestamptz NOT NULL,
        scheduled_arrival timestamptz NOT NULL,
        actual_departure timestamptz NULL,
        actual_arrival timestamptz NULL
        );
    """)


def create_table_in_postgres_db():
    postgres_hook = PostgresHook(postgres_conn_id='data_base_pg_postgres')
    result = postgres_hook.run("""
    CREATE TABLE IF NOT EXISTS myschema.flights_airflow (
    	flight_id int4 NOT NULL,
        route_no text NOT NULL,
        status text NOT NULL,
        scheduled_departure timestamptz NOT NULL,
        scheduled_arrival timestamptz NOT NULL,
        actual_departure timestamptz NULL,
        actual_arrival timestamptz NULL
        );
    """)


def get_data_from_flights(**context):
    postgres_hook = PostgresHook(postgres_conn_id='data_base_pg_demo')
    table_name = 'bookings.flights'
    sql = f"""
        select
            flight_id,
            route_no,
            status,
            scheduled_departure,
            scheduled_arrival,
            actual_departure,
            actual_arrival
        from
            {table_name}
        where
            actual_arrival >= '{context['data_interval_start'].to_date_string()}'::timestamptz
        and actual_arrival < '{context['data_interval_end'].to_date_string()}'::timestamptz
        ;
    """
    result = postgres_hook.get_pandas_df(sql)
    return result


def insert_data_into_flights_airflow(**context):
    task_name = context['task']
    if task_name == 'insert_into_flights_airflow_in_demo':
        postgres_hook = PostgresHook(postgres_conn_id='data_base_pg_demo')
        table_name = 'bookings.flights_airflow'
    if task_name == 'insert_into_flights_airflow_in_postgres':
        postgres_hook = PostgresHook(postgres_conn_id='data_base_pg_postgres')
        table_name = 'myschema.flights_airflow'
    return None


default_args = {
    'owner': 'kim-av',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='working_with_pg_db', catchup=False, schedule_interval='@daily', default_args=default_args) as dag:

    empty_task = EmptyOperator(task_id='empty_task')

    create_table_in_demo_db = PythonOperator(
        task_id='create_table_in_demo',
        python_callable=create_table_in_demo_db,
    )

    create_table_in_postgres_db = PythonOperator(
        task_id='create_table_in_postgres',
        python_callable=create_table_in_postgres_db,
    )

    get_data_from_flights_in_demo_db = PythonOperator(
        task_id='get_data_from_flights_in_demo',
        python_callable=get_data_from_flights,
    )

    empty_task >> create_table_in_demo_db >> create_table_in_postgres_db >> get_data_from_flights_in_demo_db