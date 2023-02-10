from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
from operators.cleanup_xcom import XComOperator
from airflow.models.baseoperator import chain

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "conn_id": "postgres",
    "database": "airflow",
}


with DAG(
    "cleanup_xcom",
    schedule=None,
    default_args=default_args,
    catchup=False,
    template_searchpath=["/opt/airflow/include/sql/"],
    max_active_runs=1,
):
    sql_branch_xcom = BranchSQLOperator(
        task_id="sql_branch_xcom",
        sql="SELECT CASE WHEN EXISTS ( SELECT * from xcom where EXTRACT (DAY from '{{ds}}'::timestamp - timestamp::date)>=7) THEN 1 ELSE 0 END",
        follow_task_ids_if_false=[],
        follow_task_ids_if_true=["cleanup_xcom"],
    )

    cleanup_xcom = XComOperator(task_id="cleanup_xcom")

    chain(sql_branch_xcom, cleanup_xcom)
