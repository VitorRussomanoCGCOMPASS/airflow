from pendulum import datetime
from airflow import DAG
from operators.custom_wasb import PostgresToWasbOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}



with DAG(
    dag_id="asdsa",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
):
    

    new = PostgresToWasbOperator(
        task_id="new",
        sql="SELECT britech_id FROM funds WHERE status = '{{params.active}}'",
        database='userdata',
        blob_name="teste_anbima",
        container_name="rgbrprdblob",
        params={"active": 'active'},

    )

    new

#  as_dict=True
