from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(
    dag_id="conditional_workflow_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)


def determine_branch(**context):
    execution_date = context["logical_date"]
    dow = execution_date.weekday()  # 0=Monday
    if 0 <= dow <= 2:
        return "weekday_processing"
    if 3 <= dow <= 4:
        return "end_of_week_processing"
    return "weekend_processing"


def weekday_process():
    return {
        "day_name": "Weekday",
        "task_type": "weekday",
        "record_count": 10,
    }


def end_of_week_process():
    return {
        "day_name": "EndOfWeek",
        "task_type": "end_of_week",
        "weekly_summary": "Summary generated",
    }


def weekend_process():
    return {
        "day_name": "Weekend",
        "task_type": "weekend",
        "cleanup_status": "done",
    }


start_task = EmptyOperator(task_id="start", dag=dag)

branch_task = BranchPythonOperator(
    task_id="branch_by_day",
    python_callable=determine_branch,
    dag=dag,
)

weekday_task = PythonOperator(
    task_id="weekday_processing",
    python_callable=weekday_process,
    dag=dag,
)

weekday_summary_task = EmptyOperator(task_id="weekday_summary", dag=dag)

end_of_week_task = PythonOperator(
    task_id="end_of_week_processing",
    python_callable=end_of_week_process,
    dag=dag,
)

end_of_week_report_task = EmptyOperator(task_id="end_of_week_report", dag=dag)

weekend_task = PythonOperator(
    task_id="weekend_processing",
    python_callable=weekend_process,
    dag=dag,
)

weekend_cleanup_task = EmptyOperator(task_id="weekend_cleanup", dag=dag)

end_task = EmptyOperator(
    task_id="end",
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

start_task >> branch_task
branch_task >> weekday_task >> weekday_summary_task >> end_task
branch_task >> end_of_week_task >> end_of_week_report_task >> end_task
branch_task >> weekend_task >> weekend_cleanup_task >> end_task
