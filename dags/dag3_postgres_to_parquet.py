from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

dag = DAG(
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
)

POSTGRES_CONN_ID = "postgres_default"


def check_table_exists(table_name: str):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    if not engine.dialect.has_table(engine.connect(), table_name):
        raise ValueError(f"Table {table_name} does not exist")
    df = pd.read_sql_table(table_name, engine)
    if df.empty:
        raise ValueError(f"Table {table_name} has no data")
    return True


def export_table_to_parquet(table_name: str, output_path: str, **context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql_table(table_name, engine)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", compression="snappy")

    file_size = os.path.getsize(output_path)
    return {
        "file_path": output_path,
        "row_count": len(df.index),
        "file_size_bytes": file_size,
    }


def validate_parquet(file_path: str):
    df = pd.read_parquet(file_path, engine="pyarrow")
    if df.empty:
        raise ValueError("Parquet file is empty")
    required_cols = [
        "id",
        "name",
        "age",
        "city",
        "salary",
        "join_date",
        "full_info",
        "age_group",
        "salary_category",
        "year_joined",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in parquet: {missing}")
    return True


check_table_task = PythonOperator(
    task_id="check_source_table_exists",
    python_callable=check_table_exists,
    op_kwargs={"table_name": "transformed_employee_data"},
    dag=dag,
)

export_task = PythonOperator(
    task_id="export_to_parquet",
    python_callable=export_table_to_parquet,
    op_kwargs={
        "table_name": "transformed_employee_data",
        "output_path": "/opt/airflow/output/employee_data_{{ ds }}.parquet",
    },
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_parquet_file",
    python_callable=validate_parquet,
    op_kwargs={
        "file_path": "/opt/airflow/output/employee_data_{{ ds }}.parquet",
    },
    dag=dag,
)

check_table_task >> export_task >> validate_task
