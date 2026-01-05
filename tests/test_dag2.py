from airflow.models import DagBag
from dags.dag2_data_transformation import transform_data, create_transformed_table


def _get_dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_dag2_loaded():
    dagbag = _get_dagbag()
    assert "data_transformation_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag2_structure():
    dagbag = _get_dagbag()
    dag = dagbag.dags["data_transformation_pipeline"]
    assert len(dag.tasks) == 2


def test_dag2_no_cycles():
    dagbag = _get_dagbag()
    dag = dagbag.dags["data_transformation_pipeline"]
    dag.test_cycle()


def test_dag2_schedule():
    dagbag = _get_dagbag()
    dag = dagbag.dags["data_transformation_pipeline"]
    assert dag.schedule_interval == "@daily"


def test_transform_functions_exist():
    assert callable(create_transformed_table)
    assert callable(transform_data)
