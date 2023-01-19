import json
from typing import Type

from marshmallow_sqlalchemy import SQLAlchemySchema
from operators.alchemy import SQLAlchemyOperator
from operators.anbima import AnbimaOperator
from pendulum import datetime
from sensors.anbima import AnbimaSensor
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from include.schemas.cricra import CriCraSchema
from include.schemas.debentures import DebenturesSchema
from include.schemas.ima import IMASchema
from include.schemas.vna import VNASchema

# COMPLETE : WE MUST USE USERDATA NOT POSTGRES.


def upload_task(
    data_path: str, session: Session, mshm_schema: Type[SQLAlchemySchema], many: bool
) -> None:

    with open(data_path, "r") as _file:
        data = json.load(_file)

    objs = mshm_schema(session=session).load(data, many=many)
    session.add_all(objs)


def testing(date):
    print(date)


def generate_yield_ima_b(session: Session, date: str, past_date: str):
    from flask_api.models.ima import IMA
    from flask_api.models.vna import VNA

    ima = (
        session.query(IMA).filter_by(data_referencia=date, indice="IMA-B").one_or_none()
    )
    past_ima = (
        session.query(IMA)
        .filter_by(data_referencia=past_date, indice="IMA-B")
        .one_or_none()
    )

    vna = (
        session.query(VNA)
        .filter_by(data_referencia=date, codigo_selic="760100")
        .one_or_none()
    )
    pass

    # vna.vna
    # vna.vna d-1

    # (vna.vna / vna.vna d-1 )

    # ima.yield
    # (1+ima.yield / 100)^(1/252)

    # result ima * result_vna

    # total result * total_result d-1


default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}

# TODO : TEST THE ANBIMA SENSORS
# TODO: CHECK IF ITS D OR D-1


with DAG("anbima", schedule=None, default_args=default_args, catchup=False):
    is_not_holiday = ShortCircuitOperator(
        task_id="is_business_day", python_callable=lambda: True, provide_context=True
    )

    wait_vna = AnbimaSensor(
        task_id="wait_vna",
        request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
        mode="reschedule",
        timeout=60 * 60,
    )

    fetch_vna = AnbimaOperator(
        task_id="fetch_vna",
        endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
        request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="/opt/airflow/data/anbima/vna_{{macros.ds_add(ds,-1)}}.json",
    )

    store_vna = SQLAlchemyOperator(
        task_id="store_vna",
        conn_id="postgres_userdata",
        python_callable=upload_task,
        provide_context=True,
        op_kwargs={
            "data_path": "/opt/airflow/data/anbima/vna_{{macros.ds_add(ds,-1)}}.json",
            "mshm_schema": VNASchema,
            "many": True,
        },
    )

    wait_debentures = AnbimaSensor(
        task_id="wait_debentures",
        request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
        mode="reschedule",
        timeout=60 * 60,
    )

    fetch_debentures = AnbimaOperator(
        task_id="fetch_debentures",
        endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
        request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="/opt/airflow/data/anbima/debentures_{{macros.ds_add(ds,-1)}}.json",
    )

    store_debentures = SQLAlchemyOperator(
        task_id="store_debentures",
        conn_id="postgres_userdata",
        python_callable=upload_task,
        provide_context=True,
        op_kwargs={
            "data_path": "/opt/airflow/data/anbima/debentures_{{macros.ds_add(ds,-1)}}.json",
            "mshm_schema": DebenturesSchema,
            "many": True,
        },
    )
    wait_cricra = AnbimaSensor(
        task_id="wait_cricra",
        request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
        mode="reschedule",
        timeout=60 * 60,
    )

    fetch_cricra = AnbimaOperator(
        task_id="fetch_cricra",
        endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
        request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="/opt/airflow/data/anbima/cricra_{{macros.ds_add(ds,-1)}}.json",
    )

    store_cricra = SQLAlchemyOperator(
        task_id="store_cricra",
        conn_id="postgres_userdata",
        python_callable=upload_task,
        provide_context=True,
        op_kwargs={
            "data_path": "/opt/airflow/data/anbima/cricra_{{macros.ds_add(ds,-1)}}.json",
            "mshm_schema": CriCraSchema,
            "many": True,
        },
    )

    wait_ima = AnbimaSensor(
        task_id="wait_ima",
        request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        mode="reschedule",
        timeout=60 * 60,
    )

    fetch_ima = AnbimaOperator(
        task_id="fetch_ima",
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
        output_path="/opt/airflow/data/anbima/ima_{{macros.ds_add(ds,-1)}}.json",
    )

    store_ima = SQLAlchemyOperator(
        task_id="store_ima",
        conn_id="postgres_userdata",
        python_callable=upload_task,
        provide_context=True,
        op_kwargs={
            "data_path": "/opt/airflow/data/anbima/ima_{{macros.ds_add(ds,-1)}}.json",
            "mshm_schema": IMASchema,
            "many": True,
        },
    )
    with TaskGroup(group_id="yield-ima-b") as yield_ima_b:

        calculate = PythonOperator(
            task_id="calculate",
            python_callable=testing,
            op_kwargs={"date": "{{macros.anbima_offset.forward(ds,-1)}}"},
        )
        
        post = EmptyOperator(task_id="post")  # pegar  and post to britech

    with TaskGroup(group_id="britech-indice-data") as britech:
        collect_id = EmptyOperator(task_id="collect_ids")
        get = EmptyOperator(task_id="get")
        store = EmptyOperator(task_id="store")

        collect_id.set_downstream(get)
        get.set_downstream(store)

    chain(
        is_not_holiday,
        [wait_cricra, wait_debentures, wait_ima, wait_vna],
        [fetch_cricra, fetch_debentures, fetch_ima, fetch_vna],
        [store_cricra, store_debentures, store_ima, store_vna],
    )

    yield_ima_b.set_upstream([store_ima, store_vna])
    yield_ima_b.set_downstream(britech)

# COMPLETE : PYTHONPATH TO INCLUDE AND TO MODELS!\
# TODO : CALCULATE YIELD IMA B (IMA-B YIELD FROM ; VNA NTN-B)
# TODO : WRITE POST YIELD IMA B
