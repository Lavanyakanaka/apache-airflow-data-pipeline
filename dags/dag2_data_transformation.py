from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

dag = DAG(
    dag_id="data_transformation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

POSTGRES_CONN_ID = "postgres_default"
SRC_TABLE = "raw_employee_data"
TGT_TABLE = "transformed_employee_data"


def create_transformed_table():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TGT_TABLE} (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary FLOAT,
        join_date DATE,
        full_info VARCHAR(500),
        age_group VARCHAR(20),
        salary_category VARCHAR(20),
        year_joined INTEGER
    );
    """
    hook.run(create_sql)


def transform_data():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql_table(SRC_TABLE, engine)

    df["full_info"] = df["name"] + " - " + df["city"]

    def age_group(age):
        if age < 30:
            return "Young"
        if age < 50:
            return "Mid"
        return "Senior"

    df["age_group"] = df["age"].apply(age_group)

    def salary_cat(s):
        if s < 50000:
            return "Low"
        if s < 80000:
            return "Medium"
        return "High"

    df["salary_category"] = df["salary"].apply(salary_cat)
    df["year_joined"] = pd.to_datetime(df["join_date"]).dt.year.astype(int)

    rows_processed = len(df.index)

    df.to_sql(TGT_TABLE, engine, if_exists="replace", index=False)

    return {
        "rows_processed": rows_processed,
        "rows_inserted": rows_processed,
    }


create_transformed_table_task = PythonOperator(
    task_id="create_transformed_table",
    python_callable=create_transformed_table,
    dag=dag,
)

transform_and_load_task = PythonOperator(
    task_id="transform_and_load",
    python_callable=transform_data,
    dag=dag,
)

create_transformed_table_task >> transform_and_load_task
