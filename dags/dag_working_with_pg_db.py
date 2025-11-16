from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from pendulum import datetime, parse, now
from airflow.operators.empty import EmptyOperator
from pandas import read_csv
from airflow.models import Variable
from os import listdir, path, remove

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
    load_date = Variable.get("flights_load_date")
    if not load_date:
        date_from = context['data_interval_start'].to_date_string().replace('-', '_')
        date_to = context['data_interval_end'].to_date_string().replace('-', '_')
    else:
        date_from = load_date.replace('-', '_')
        date_to = parse(load_date).add(days=1).to_date_string().replace('-', '_')
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
            actual_arrival >= '{date_from}'::timestamptz
        and actual_arrival < '{date_to}'::timestamptz
        ;
    """
    result = postgres_hook.get_pandas_df(sql)
    file_name = date_from + '-' + date_to
    file_path = f"/opt/airflow/data/flights_{file_name}.csv"
    result.to_csv(file_path, mode="w", header=True, index=False, encoding='utf-8', sep=';')
    print(f"Created file: flights_{file_name}.csv")


def update_data_into_flights_airflow(**context):
    task_name = context['task'].task_id
    postgres_hook = PostgresHook(postgres_conn_id='data_base_pg_demo')
    load_date = Variable.get("flights_load_date")
    if not load_date:
        date_from = context['data_interval_start'].to_date_string().replace('-', '_')
        date_to = context['data_interval_end'].to_date_string().replace('-', '_')
    else:
        date_from = load_date.replace('-', '_')
        date_to = parse(load_date).add(days=1).to_date_string().replace('-', '_')
    file_name = date_from + '-' + date_to
    file_path = f"/opt/airflow/data/flights_{file_name}.csv"
    if task_name == 'insert_into_flights_airflow_in_demo':
        postgres_hook = PostgresHook(postgres_conn_id='data_base_pg_demo')
        table_name = 'bookings.flights_airflow'
    if task_name == 'insert_into_flights_airflow_in_postgres':
        postgres_hook = PostgresHook(postgres_conn_id='data_base_pg_postgres')
        table_name = 'myschema.flights_airflow'

    if not table_name:
        print("ERROR: Unknown task id.")
        return None

    df = read_csv(file_path, encoding='utf-8', delimiter=';')
    if df.empty:
        print("ERROR: Empty dataframe — nothing to insert.")
        return None

    # 1. DELETE старых данных
    delete_sql = f"""
        delete 
        from {table_name} 
        where 
            actual_arrival >= '{date_from}'::timestamptz 
        and actual_arrival < '{date_to}'::timestamptz
        ;
    """
    postgres_hook.run(delete_sql)

    # 2. INSERT
    columns_list = ', '.join(df.columns)
    values_list = []
    for row in df.itertuples(index=False, name=None):
        v = ", ".join(f"'{str(x)}'" for x in row)
        values_list.append(f"({v})")

    insert_sql = f"""
        insert into {table_name}
        ({columns_list})
        values
        {', '.join(values_list)}
    """
    postgres_hook.run(insert_sql)
    print(f"Inserted {len(df)} rows into {table_name} table")


def delete_old_files(**context):
    """
    Файл считается устарелым, если его дата < 3 месяцев от текущей даты
    """
    delete_flag = Variable.get("flights_delete_old_files")
    if not delete_flag:
        today_date_str = now().date().add(months=-3).to_date_string().replace('-', '_')
        folder_path = "/opt/airflow/data"
        files = [file for file in listdir(folder_path) if 'flights_' in file]
        for file in files:
            if today_date_str in file:
                remove(path.join(folder_path, file))
                print(f"Deleted file {file}")
    else:
        print("With option: No old files deleted.")
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

    insert_data_into_flights_in_demo_db = PythonOperator(
        task_id='insert_into_flights_airflow_in_demo',
        python_callable=update_data_into_flights_airflow,
    )

    insert_data_into_flights_in_postgres_db = PythonOperator(
        task_id='insert_into_flights_airflow_in_postgres',
        python_callable=update_data_into_flights_airflow,
    )

    delete_old_files_from_data_folder = PythonOperator(
        task_id = 'delete_old_files_from_data_folder',
        python_callable=delete_old_files,
    )

    empty_task >> create_table_in_demo_db
    create_table_in_demo_db >> create_table_in_postgres_db
    create_table_in_postgres_db >> get_data_from_flights_in_demo_db
    get_data_from_flights_in_demo_db >> [insert_data_into_flights_in_demo_db, insert_data_into_flights_in_postgres_db]
    [insert_data_into_flights_in_demo_db, insert_data_into_flights_in_postgres_db] >> delete_old_files_from_data_folder