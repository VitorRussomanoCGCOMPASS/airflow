from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.file_share import FileShareOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook




default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "conn_id": "postgres",
    "database": "userdata",
    "mode": "reschedule",
    "timeout": 60 * 30,
    "max_active_runs": 1,
    "catchup": False,
}


def _print_conn(conn_id: Connection):
    return BaseHook.get_connection(conn_id).get_uri()


with DAG(
    dag_id="asdsa",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    render_template_as_native_obj=True,
):

    print_conn = PythonOperator(
        task_id="print_conn",
        python_callable=_print_conn,
        op_kwargs={"conn_id": "email_default"},
    )
