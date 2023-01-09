from operators.anbima import AnbimaOperator
from pendulum import datetime
from sensors.anbima import AnbimaSensor
from utils.hol import _is_not_holiday

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}

# TODO : MOVE IT FROM PLUGINS.


with DAG('resultados_ima', schedule="@daily", default_args=default_args, catchup=False):
    holiday = PythonOperator(
        task_id="is_not_holiday", python_callable=_is_not_holiday, provide_context=True
    )

    wait = AnbimaSensor(
        task_id="wait_for_data",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint='/feed/precos-indices/v1/indices-mais/resultados-ima',
        mode="reschedule",
        timeout=60 * 60,
        data=None,
        response_check=None,
        extra_options=None,
    )

    fetch = AnbimaOperator(
        task_id="fetch_data",
        endpoint='/feed/precos-indices/v1/indices-mais/resultados-ima',
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="C:/Users/Vitor Russomano/airflow/data/custom_operator/'{{macros.ds_add(ds,-1)}}'.json",
    )


    holiday.set_downstream(wait)
    wait.set_downstream(fetch)
    