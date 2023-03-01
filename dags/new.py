from pendulum import datetime
from airflow import DAG
from operators.custom_wasb import PostgresToWasbOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


def _generate():
    return "ativo"


with DAG(
    dag_id="asdsa",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    render_template_as_native_obj=True,
):

    generate = PythonOperator(
        task_id="generate", python_callable=_generate, do_xcom_push=True
    )

    new = PostgresToWasbOperator(
        task_id="new",
        sql="teste.sql",
        database="userdata",
        blob_name="teste_anbima",
        container_name="rgbrprdblob",
        params={"active": generate.output},
    )

    generate >> new

#  as_dict=True
