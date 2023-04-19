from operators.custom_sql import PostgresOperator
from pendulum import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import BranchSQLOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "conn_id": "postgres",
    "database": "airflow",
}


doc_md_DAG = """ 
# Delete XCOM
This process is responsible for cleaning all data older than 7 days of the XCOM table.
* sql_branch_xcom : Checks if there is any data older than 7 days. If there is, calls for cleanup_xcom. Else, just skips all following tasks.
* delete_xcom : Cleans up all data older than 7 days from the XCOM table.
"""

with DAG(
    "delete_xcom",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    doc_md=doc_md_DAG,
):
    sql_branch_xcom = BranchSQLOperator(
        task_id="sql_branch_xcom",
        sql="SELECT CASE WHEN EXISTS ( SELECT * from xcom where EXTRACT (DAY from '{{ds}}'::timestamp - timestamp::date)>=7) THEN 1 ELSE 0 END",
        follow_task_ids_if_false=[],
        follow_task_ids_if_true=["delete_xcom"],
    )

    delete_xcom = PostgresOperator(
        task_id="delete_xcom",
        sql="DELETE FROM XCOM where EXTRACT (DAY from '{{ds}}'::timestamp - timestamp::date)>=7 ",
    )

    chain(sql_branch_xcom, delete_xcom)
