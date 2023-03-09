from pendulum import datetime
from airflow import DAG

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
    pass
