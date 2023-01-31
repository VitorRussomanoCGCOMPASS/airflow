from airflow import DAG
from pendulum import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "template_searchpath": "/opt/airflow/include/",
}

with DAG(
    "update_sql",
    schedule=None,
    default_args=default_args,
    catchup=False,
):
    # TODO : POSTGRES SELECT INDEX THAT DO NOT HAVE DATA IN THE CURRENT DATE
    check_for_missing = PostgresOperator(
        task_id="check_for_missing",
        postgres_conn_id="postgres_userdata",
        sql="sql/check_indices.sql",
    )


with DAG("push_to_table", schedule=None, start_date=datetime(2023, 1, 1)):

    push_to_table = PostgresOperator(
        task_id="push_to_table",
        postgres_conn_id="postgres_userdata",
        sql="from '/opt/data/britech/operacoes/2023-01-26.json' delimiter ',' csv header",
    )
