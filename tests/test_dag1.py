from airflow.models import DagBag


def _get_dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_dag1_loaded():
    dagbag = _get_dagbag()
    assert "csv_to_postgres_ingestion" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag1_structure():
    dagbag = _get_dagbag()
    dag = dagbag.dags["csv_to_postgres_ingestion"]
    assert len(dag.tasks) == 3


def test_dag1_task_dependencies():
    dagbag = _get_dagbag()
    dag = dagbag.dags["csv_to_postgres_ingestion"]

    create_task = dag.get_task("create_table_if_not_exists")
    truncate_task = dag.get_task("truncate_table")
    load_task = dag.get_task("load_csv_to_postgres")

    assert truncate_task in create_task.downstream_list
    assert load_task in truncate_task.downstream_list


def test_dag1_no_cycles():
    dagbag = _get_dagbag()
    dag = dagbag.dags["csv_to_postgres_ingestion"]
    dag.test_cycle()


def test_dag1_schedule():
    dagbag = _get_dagbag()
    dag = dagbag.dags["csv_to_postgres_ingestion"]
    assert dag.schedule_interval == "@daily"
