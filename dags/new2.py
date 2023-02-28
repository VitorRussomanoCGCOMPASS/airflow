from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
from operators.custom_wasb import PostgresToWasbOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}



with DAG(
    "example_dag",
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
) as dag:

  

    teste = PostgresToWasbOperator(
        task_id="teste",
        conn_id = 'postgres',
        database='userdata',
        blob_name="",
        container_name="",
        sql="select * from funds where status = 'ativo'",
        dict_cursor = True
    )

