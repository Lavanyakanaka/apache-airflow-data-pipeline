from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

dag = DAG(
    dag_id="csv_to_postgres_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

POSTGRES_CONN_ID = "postgres_default"
TABLE_NAME = "raw_employee_data"
CSV_PATH = "/opt/airflow/data/input.csv"


def create_employee_table():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary FLOAT,
        join_date DATE
    );
    """
    hook.run(create_sql)


def truncate_employee_table():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(f"TRUNCATE TABLE {TABLE_NAME};")


def load_csv_data():
    df = pd.read_csv(CSV_PATH)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    return len(df.index)


create_table_task = PythonOperator(
    task_id="create_table_if_not_exists",
    python_callable=create_employee_table,
    dag=dag,
)

truncate_table_task = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_employee_table,
    dag=dag,
)

load_csv_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_data,
    dag=dag,
)

create_table_task >> truncate_table_task >> load_csv_task
