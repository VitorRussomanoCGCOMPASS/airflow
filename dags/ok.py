from pendulum import datetime
from airflow.operators.python import PythonOperator
from operators.custom_sql import MSSQLOperator
from sensors.sql import SqlSensor
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.decorators import task

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}




with DAG(
    "great",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/mssql/"],
):

    pass

