from airflow.models import DagBag
def test_all_dags_load():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    expected_dags = [
        "csv_to_postgres_ingestion",
        "data_transformation_pipeline",
        "postgres_to_parquet_export",
        "conditional_workflow_pipeline",
        "notification_workflow",
    ]
    for dag_id in expected_dags:
        assert dag_id in dagbag.dags, f"DAG {dag_id} not found"
    assert len(dagbag.import_errors) == 0


def test_no_import_errors():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dagbag.import_errors) == 0


def test_all_dag_ids_unique():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag_ids = [dag.dag_id for dag in dagbag.dags.values()]
    assert len(dag_ids) == len(set(dag_ids))
