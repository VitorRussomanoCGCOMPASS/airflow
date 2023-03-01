from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator


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
    template_searchpath=["/opt/airflow/include/sql/etl/"],
    render_template_as_native_obj=True,
):

    create_audit_table = SQLExecuteQueryOperator(
        task_id="create_audit_table",
        conn_id="postgres",
        database="userdata",
        sql="create_audit_table.sql",
        do_xcom_push=False,
        params={"table": "currency_values"},
    )

    create_audit_table
