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
from airflow.providers.sendgrid.utils.emailer import send_email

from operators.write_audit_publish import InsertSQLOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "mode": "reschedule",
    "timeout": 60 * 30,
    "max_active_runs": 1,
    "catchup": False,
}


def _print(**kwargs):
    print("OKe")


with DAG(
    dag_id="asdsa",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    render_template_as_native_obj=True,
):
    tes = InsertSQLOperator(
        task_id='tess',
        table="Employee_temp",
        values={"EmployeeID": 12},
        database="DB_Brasil",
        conn_id="mssql_default",
    )
