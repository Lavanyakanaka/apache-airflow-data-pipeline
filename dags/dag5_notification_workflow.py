from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.utils.context import Context
import logging
import pendulum

dag = DAG(
    dag_id="notification_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)


def send_success_notification(context: Context):
    ti = context["ti"]
    msg = f"Task {ti.task_id} succeeded for run {context['run_id']}"
    logging.info(msg)
    return {
        "notification_type": "success",
        "status": "sent",
        "message": msg,
        "timestamp": pendulum.now().to_iso8601_string(),
    }


def send_failure_notification(context: Context):
    ti = context["ti"]
    err = context.get("exception")
    msg = f"Task {ti.task_id} failed: {err}"
    logging.error(msg)
    return {
        "notification_type": "failure",
        "status": "sent",
        "message": msg,
        "error": str(err),
        "timestamp": pendulum.now().to_iso8601_string(),
    }


def risky_operation(**context):
    execution_date = context["logical_date"]
    day = execution_date.day
    if day % 5 == 0:
        raise Exception(f"Simulated failure: day {day} divisible by 5")
    return {
        "status": "success",
        "execution_date": execution_date.to_date_string(),
        "success": True,
    }


def cleanup_task():
    ts = pendulum.now().to_iso8601_string()
    logging.info("Running cleanup")
    return {"cleanup_status": "completed", "timestamp": ts}


start_task = EmptyOperator(task_id="start_task", dag=dag)

risky_task = PythonOperator(
    task_id="risky_operation",
    python_callable=risky_operation,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag,
)

success_notification_task = EmptyOperator(
    task_id="success_notification",
    trigger_rule="all_success",
    dag=dag,
)

failure_notification_task = EmptyOperator(
    task_id="failure_notification",
    trigger_rule="all_failed",
    dag=dag,
)

always_execute_task = PythonOperator(
    task_id="always_execute",
    python_callable=cleanup_task,
    trigger_rule="all_done",
    dag=dag,
)

start_task >> risky_task >> [success_notification_task, failure_notification_task]
[success_notification_task, failure_notification_task] >> always_execute_task
