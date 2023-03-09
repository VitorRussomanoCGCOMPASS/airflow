from pendulum import datetime
from airflow import DAG
from operators.write_audit_publish import TemporaryTableSQLOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}

with DAG(
    dag_id="etl_dag",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    render_template_as_native_obj=True,
):
    temp = TemporaryTableSQLOperator(
        database="DB_Brasil",
        conn_id="mssql_default",
        table="dbo.Employee",
        task_id="temp",
    )
    temp
