from sensors.anbima import AnbimaSensor
from operators.anbima import AnbimaOperator
from airflow import DAG

from pendulum import duration
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
from utils.hol import _is_not_holiday

from airflow.operators.python import PythonOperator


default_args = {"retries": 1, "retry_delay": duration(minutes=5)}

with DAG(
    dag_id="anbima_dynamic",
    default_args=default_args,
    description=None,
    start_date=datetime(2022, 1, 1),
    schedule="@daily",
    catchup=False,
):

    holiday = PythonOperator(task_id="is_not_holiday",python_callable=_is_not_holiday,provide_context=True)


    wait = AnbimaSensor(
        task_id="wait_for_data",
        headers={"data": "{{prev_ds}}"},
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        mode="reschedule",
        timeout=60 * 60,
        data=None,
        response_check=None,
        extra_options=None,
    )

    fetch = AnbimaOperator(
        task_id="fetch_data",
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        headers={"data": "{{prev_ds}}"},
        output_path="C:/Users/Vitor Russomano/airflow/data/custom_operator/{{prev_ds}}.json",
    )
    
    
    pull = EmptyOperator(task_id="pull")

    holiday.set_downstream(wait)
    wait.set_downstream(fetch)
    fetch.set_downstream(pull)
