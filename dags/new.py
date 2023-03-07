from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from operators.custom_wasb import MSSQLToWasbOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


def _generate():
    logging.info("teste")
    return "ativo"


with DAG(
    dag_id="asdsa",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    render_template_as_native_obj=True,
):

    generate = MSSQLToWasbOperator(
        conn_id="mssql_default",
        sql="USE DB_Brasil SELECT * FROM dbo.Employee",
        task_id="gen",
        database="DB_Brasil",
        blob_name='teste',
        container_name='rgbrprdblob',
    )

    generate

#  as_dict=True