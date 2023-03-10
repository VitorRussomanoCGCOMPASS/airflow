from pendulum import datetime
from airflow import DAG
from operators.write_audit_publish import TemporaryTableSQLOperator
from operators.custom_wasb import MSSQLOperator , PostgresOperator


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
    temp = MSSQLOperator(
        conn_id="mssql_default",
        database= 'DB_Brasil',
        task_id="temp",
        sql ='select EmployeeID from Employee',
    )
    temp


a = [(('EmployeeID', 'int', None, 10, 10, 0, False),)]
