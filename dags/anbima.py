from operators.anbima import AnbimaOperator
from pendulum import datetime
from sensors.anbima import AnbimaSensor
from utils.hol import _is_not_holiday
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain




default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}

from airflow.utils.context import Context


with DAG("anbima", schedule="@daily", default_args=default_args, catchup=False):
    is_not_holiday = PythonOperator(
        task_id="is_not_holiday", python_callable=_is_not_holiday, provide_context=True
    )

    wait_vna = AnbimaSensor(
        task_id="wait_vna",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
        mode="reschedule",
        timeout=60 * 60,
        data=None,
        response_check=None,
        extra_options=None,
    )

    fetch_vna = AnbimaOperator(
        task_id="fetch_vna",
        endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="C:/Users/Vitor Russomano/airflow/data/anbima/'vna_{{macros.ds_add(ds,-1)}}'.json",
    )

    post_vna = EmptyOperator(
        task_id='post_vna')

    wait_debentures = AnbimaSensor(
        task_id="wait_debentures",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
        mode="reschedule",
        timeout=60 * 60,
        data=None,
        response_check=None,
        extra_options=None,
    )

    fetch_debentures = AnbimaOperator(
        task_id="fetch_debentures",
        endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="C:/Users/Vitor Russomano/airflow/data/anbima/'debentures_{{macros.ds_add(ds,-1)}}'.json",
    )

    post_debentures = EmptyOperator(task_id="post_debentures")

    wait_cricra = AnbimaSensor(
        task_id="wait_cricra",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
        mode="reschedule",
        timeout=60 * 60,
        data=None,
        response_check=None,
        extra_options=None,
    )

    fetch_cricra = AnbimaOperator(
        task_id="fetch_cricra",
        endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="C:/Users/Vitor Russomano/airflow/data/anbima/'cricra_{{macros.ds_add(ds,-1)}}'.json",
    )

    post_cricra = EmptyOperator(task_id="post_cricra")

    wait_ima = AnbimaSensor(
        task_id="wait_ima",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        mode="reschedule",
        timeout=60 * 60,
        data=None,
        response_check=None,
        extra_options=None,
    )

    fetch_ima = AnbimaOperator(
        task_id="fetch_ima",
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        headers={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="C:/Users/Vitor Russomano/airflow/data/anbima/'ima_{{macros.ds_add(ds,-1)}}'.json", # /opt/data/
    )

    post_ima = EmptyOperator(task_id="post_ima")

    with TaskGroup(group_id="yield-ima-b") as yield_ima_b:
        read = EmptyOperator(task_id="read")
        transform = EmptyOperator(task_id="transform")
        post = EmptyOperator(task_id="post")
        
        read.set_downstream(transform)
        transform.set_downstream(post)


    end = EmptyOperator(task_id="end")

    is_not_holiday.set_downstream([wait_debentures, wait_vna, wait_ima, wait_cricra])
    chain(
        [wait_cricra, wait_debentures],
        [fetch_cricra, fetch_debentures],
        [post_cricra, post_debentures],
        end,
    )

    chain([wait_ima, wait_vna], [fetch_ima, fetch_vna], yield_ima_b)
    chain([fetch_ima, fetch_vna], [post_ima, post_vna], end)
 