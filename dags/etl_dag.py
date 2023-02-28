from pendulum import datetime
from airflow import DAG
from operators.custom_wasb import PostgresToWasbOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


def teste():
    return {"date": "2023-01-28", "value": 10, "currency_id": 1027}


with DAG(
    dag_id="etl_dag",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    render_template_as_native_obj=True,
):
    extract = PythonOperator(
        task_id="extract", python_callable=teste, do_xcom_push=False
    )

    create_audit_table = SQLExecuteQueryOperator(
        task_id="create_audit_table",
        conn_id="postgres",
        database="userdata",
        sql="SELECT * INTO temporary_table from currency_values where 1=0",
        do_xcom_push=False,
    )

    insert_into_audit_table = SQLExecuteQueryOperator(
        task_id="insert_into_audit_table",
        conn_id="postgres",
        database="userdata",
        sql="INSERT INTO temporary_table (date, value, currency_id) VALUES ('2023-01-28' ,10 , 1027)",
        do_xcom_push=False,
    )

    validate = SQLColumnCheckOperator(
        task_id="col_checks",
        conn_id="postgres",
        database="userdata",
        table="temporary_table",
        column_mapping={"value": {"min": {"greater_than": 1}}},
    )

    merge_tables = SQLExecuteQueryOperator(
        task_id="merge_tables",
        conn_id="postgres",
        database="userdata",
        sql="INSERT INTO currency_values (SELECT * FROM temporary_table WHERE date NOT IN (SELECT date FROM currency_values));",
        do_xcom_push=False,
    )
    drop_audit_table = SQLExecuteQueryOperator(
        task_id="drop_audit_table",
        conn_id="postgres",
        database="userdata",
        sql="DROP TABLE IF EXISTS temp",
        do_xcom_push=False,
    )

    (
        extract
        >> create_audit_table
        >> insert_into_audit_table
        >> validate
        >> merge_tables
        >> drop_audit_table
    )
