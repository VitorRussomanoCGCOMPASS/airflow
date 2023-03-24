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

from operators.write_audit_publish import TemporaryTableSQLOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "mode": "reschedule",
    "timeout": 60 * 30,
    "max_active_runs": 1,
    "catchup": False,
}


def _printa(**kwargs):
    import pandas

    return pandas.DataFrame.from_dict({"a": [1, 1], "b": [2, 2]})


import pandas

isinstance(_printa(), pandas.DataFrame)


with DAG(
    dag_id="asdsa",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    render_template_as_native_obj=True,
):

    ok = SqlSensor(
        task_id='fuck_mssql',
        conn_id="mssql-default",
        sql="""WITH WorkTable(id) as (select britech_id from funds WHERE britech_id  = any(array[3, 10, 17, 30, 32, 42, 49]))
                SELECT ( SELECT COUNT(*)
                FROM funds_values
                WHERE "IdCarteira" IN ( SELECT id FROM WorkTable) 
	            AND "Data" ='2023-03-22') =
	   		    (SELECT COUNT(*) FROM WorkTable)
                 (with parameters {'ids': [3, 10, 17, 30, 32, 42, 49]})""",
    hook_params={"database": "DB_Brasil"},
    )
