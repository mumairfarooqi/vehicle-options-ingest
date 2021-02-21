from airflow.models import DagBag, Variable
from airflow.contrib.hooks.gcp_sql_hook import CloudSqlDatabaseHook
from airflow.configuration import get_airflow_home


def test_dag_validity(mocker):
    mocker.patch.object(Variable, "get", return_value="some-value")

    mocker.patch.object(CloudSqlDatabaseHook, "get_connections", return_value=None)

    dagbag = DagBag(dag_folder=f"{get_airflow_home()}/dags")

    assert not dagbag.import_errors
