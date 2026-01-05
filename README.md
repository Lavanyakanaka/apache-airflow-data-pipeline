# Apache Airflow Data Pipeline

This project implements a complete Airflow data pipeline running in Docker with PostgreSQL as both the Airflow metadata DB and the data warehouse. [page:1]

## Architecture

- DAG 1: Ingests CSV employee data from `/opt/airflow/data/input.csv` into the `raw_employee_data` table in PostgreSQL. [page:1]
- DAG 2: Transforms `raw_employee_data` into `transformed_employee_data` by adding `full_info`, `age_group`, `salary_category`, and `year_joined` columns. [page:1]
- DAG 3: Exports `transformed_employee_data` to a Parquet file in `/opt/airflow/output` using pyarrow and snappy compression. [page:1]
- DAG 4: Demonstrates conditional branching logic based on day of week. [page:1]
- DAG 5: Demonstrates success/failure notifications and cleanup with Airflow callbacks and trigger rules. [page:1]

## Prerequisites

- Docker and Docker Compose installed on your machine. [web:7][web:10]
- Git (to clone the repository). [web:15]

## Setup Instructions

1. Clone the repository:

   ```bash
   git clone https://github.com/Lavanyakanaka/apache-airflow-data-pipeline.git
   cd apache-airflow-data-pipeline
