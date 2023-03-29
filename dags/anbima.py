import logging
from typing import Any, Type

from marshmallow_sqlalchemy import SQLAlchemySchema
from operators.alchemy import SQLAlchemyOperator
from operators.api import AnbimaOperator
from pendulum import datetime
from sensors.anbima import AnbimaSensor
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from include.schemas.cricra import CriCraSchema
from include.schemas.debentures import DebenturesSchema
from include.schemas.ima import IMASchema
from include.schemas.vna import VNASchema
from include.utils.is_business_day import _is_business_day


def _upload_task(
    data: Any, session: Session, mshm_schema: Type[SQLAlchemySchema], many: bool
) -> None:

    objs = mshm_schema(session=session).load(data, many=many)
    logging.info("Adding objects to session")
    session.add_all(objs)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "mode": "reschedule",
    "timeout": 60 * 60,
    "catchup": False,
}


with DAG(
    "anbima",
    schedule=None,
    default_args=default_args,
):
    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    with TaskGroup(group_id="fetch_from_anbima") as fetch_from_anbima:

        wait_vna = AnbimaSensor(
            task_id="wait_vna",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts), -1)}}"
            },
            endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
        )

        fetch_vna = AnbimaOperator(
            task_id="fetch_vna",
            endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
        )

        store_vna = SQLAlchemyOperator(
            task_id="store_vna",
            conn_id="postgres_userdata",
            python_callable=_upload_task,
            provide_context=True,
            op_kwargs={
                "data": fetch_vna.output,
                "mshm_schema": VNASchema,
                "many": True,
            },
        )

        wait_debentures = AnbimaSensor(
            task_id="wait_debentures",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
            endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
        )

        fetch_debentures = AnbimaOperator(
            task_id="fetch_debentures",
            endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
        )

        store_debentures = SQLAlchemyOperator(
            task_id="store_debentures",
            conn_id="postgres_userdata",
            python_callable=_upload_task,
            provide_context=True,
            op_kwargs={
                "data": fetch_debentures.output,
                "mshm_schema": DebenturesSchema,
                "many": True,
            },
        )
        wait_cricra = AnbimaSensor(
            task_id="wait_cricra",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
            endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
        )

        fetch_cricra = AnbimaOperator(
            task_id="fetch_cricra",
            endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
        )

        store_cricra = SQLAlchemyOperator(
            task_id="store_cricra",
            conn_id="postgres_userdata",
            python_callable=_upload_task,
            provide_context=True,
            op_kwargs={
                "data": fetch_cricra.output,
                "mshm_schema": CriCraSchema,
                "many": True,
            },
        )

        wait_ima = AnbimaSensor(
            task_id="wait_ima",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
            endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        )

        fetch_ima = AnbimaOperator(
            task_id="fetch_ima",
            endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
        )

        store_ima = SQLAlchemyOperator(
            task_id="store_ima",
            conn_id="postgres_userdata",
            python_callable=_upload_task,
            provide_context=True,
            op_kwargs={
                "data": fetch_ima.output,
                "mshm_schema": IMASchema,
                "many": True,
            },
        )

        chain(
            [wait_cricra, wait_debentures, wait_ima, wait_vna],
            [fetch_cricra, fetch_debentures, fetch_ima, fetch_vna],
            [store_cricra, store_debentures, store_ima, store_vna],
        )

    chain(is_business_day, fetch_from_anbima)


""" 

    with TaskGroup(group_id="yield-ima-b") as yield_ima_b:
        @task
        def generate_yield_ima_b(session, date: str, past_date: str):
            from flask_api.models.ima import IMA
            from flask_api.models.vna import VNA
            from flask_api.models.indexes import IndexValues

            ima = (
                session.query(IMA).filter_by(data_referencia=date, indice="IMA-B").one_or_none()
            )

            vna = (
                session.query(VNA)
                .filter_by(data_referencia=date, codigo_selic="760100")
                .one_or_none()
            )

            past_vna = (
                session.query(VNA)
                .filter_by(data_referencia=past_date, codigo_selic="760100")
                .one_or_none()
            )

            # TODO : REFER TO THE PROPER INDEX

            vna_diff = vna.vna / past_vna.vna

            ima_yield = (1 + ima.yild / 100) ^ (1 / 252)

            yield_plus_vna = ima_yield * vna_diff

            
            past_pu = (
                session.query(IndexValues)
                .filter_by(data_referencia=past_date, index="PU")
                .one_or_none()
            )

            past_pu.value * yield_plus_vna

            # TODO : MERGE !

            if (
                session.query(IndexValues)
                .filter_by(data_referencia=date, index="PU")
                .one_or_none()
            ):
                session.merge(
                    IndexValues
                )  # result ima * result_vna total rsesult * total_result d-1

        generated_yield_ima_b = generate_yield_ima_b()
        calculate = EmptyOperator(task_id="calculate")

        # PASS USING XCOM.
    
        post_to_britech = EmptyOperator(
            task_id="post_to_britech"
        )

        chain(calculate, post_to_britech)

    chain([store_ima, store_vna], latest_only, yield_ima_b)

 """

""" 
select ((1 +a.yield / 100 ) ^1/252) dailyima 
from ima_anbima a 
where a.indice = 'IMA-B'
and a.data_referencia = '2023-02-15'

 """


""" 
 
SELECT ( 
	select vna from anbima_vna
where codigo_selic= '760100'
and data_referencia = '2023-02-15')
	/
	(
	select vna from anbima_vna
where codigo_selic= '760100'
and data_referencia = '2023-02-14') as vna_ratio
 """


""" 
 
WITH VNA AS (SELECT ( 
	select vna from anbima_vna
where codigo_selic= '760100'
and data_referencia = '2023-02-15')
	/
	(
	select vna from anbima_vna
where codigo_selic= '760100'
and data_referencia = '2023-02-14') as vna_ratio )
SELECT VNA.vna_ratio * ((1 +a.yield / 100 ) ^1/252) dailyima 
FROM ima_anbima a , VNA 
where a.indice = 'IMA-B'
and a.data_referencia = '2023-02-15'

-- STILL HAVE TO GET LAST PU AND MULTIPLY THIS VALUE.
"""
