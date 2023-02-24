from operators.alchemy import SQLAlchemyOperator
from operators.britech import BritechOperator
from pendulum import datetime
from sensors.britech import BritechEmptySensor
from sqlalchemy.orm import Session
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLExecuteQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from include.utils.is_business_day import _is_business_day
from airflow.operators.latest_only import LatestOnlyOperator
from operators.extended_sql import SQLQueryToLocalOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "catchup": False,
    "max_active_runs": 1,
    "conn_id": "postgres",
    "database": "userdata",
}


def _push_cotista_op(file_path: str, session: Session) -> None:
    """
    Reads json data from file specified by file_path and insert but do not update to CotistaOp table

    Parameters
    ----------
    file_path : str
    session : Session
    """
    import json
    import logging

    # FIXME : COTISTA OP
    from flask_api.models.cotista_op import CotistaOP
    from sqlalchemy.dialects.postgresql import insert as pgs_upsert

    with open(file_path, "r") as _file:
        logging.info("Getting file.")
        data = json.load(_file)

    stmt = pgs_upsert(CotistaOP).values(data)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["IdOperacao"],
    )

    session.execute(stmt)
    logging.info("Writing Cotista Operations to database")


with DAG(
    dag_id="cotista_op",
    schedule=None,
    default_args=default_args,
    template_searchpath = ["/opt/airflow/include/sql/"],
):

    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )
    with TaskGroup(group_id="new_cotista_op") as new_cotista_op:
        
        cotista_op_sensor = BritechEmptySensor(
            task_id="sensor_cotista_op",
            endpoint="/Fundo/OperacaoCotistaAnalitico",
            request_params={
                "dataInicio": "2023-02-15",
                "dataFim": "2023-02-15",
                "idsCarteiras": ';',
            },
            mode="reschedule",
            timeout=60 * 30,
        )

        fetch_cotista_op = BritechOperator(
            task_id="fetch_cotista_op",
            endpoint="/Fundo/OperacaoCotistaAnalitico",
            output_path="/opt/airflow/data/britech/operacoes",
            filename="{{ds}}.json",
            request_params={
                "dataInicio": "{{ds}}",
                "dataFim": "{{ds}}",
                "idsCarteiras": ';',
            },
            do_xcom_push=False,
        )

        push_cotista_op = SQLAlchemyOperator(
            task_id="push_cotista_op",
            python_callable=_push_cotista_op,
            op_kwargs={"file_path": "/opt/airflow/data/britech/operacoes/{{ds}}.json"},
            depends_on_past=True,
        )

        chain(
            is_business_day,
            cotista_op_sensor,
            fetch_cotista_op,
            push_cotista_op,
        )

    with TaskGroup(group_id="update_cotista_op") as update_cotista_op:

        latest_only = LatestOnlyOperator(task_id="latest_only")

        # COMPLETE : NOT ONLY D-1. BUT ALL PAST DATES.
        look_for_updates = BranchSQLOperator(
            task_id="look_for_updates",
            sql="op_check_for_update.sql",
            follow_task_ids_if_false=["latest_only_join"],
            follow_task_ids_if_true=["update_cotista_op"],
        )

        # TODO : UPDATE BY COTIZACAO DATE.
        update_cotista_op = SQLExecuteQueryOperator(
            task_id="update_cotista_op",
            sql="op_update.sql",
        )

        chain(
            is_business_day,
            latest_only,
            look_for_updates,
            update_cotista_op,
        )

    latest_only_join = LatestOnlyOperator(
        task_id="latest_only_join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    with TaskGroup(group_id="daily_flow_report") as daily_flow_report:

        check_data = SQLCheckOperator(
            task_id="check_data",
            sql="check_funds.sql",
        )

        request_data = SQLQueryToLocalOperator(
            task_id="request_data",
            file_path="/opt/airflow/data/cotista_op_{{ds}}.json",
            sql="",
        )

        render_to_template = EmptyOperator(task_id="render_to_template")
        chain(check_data, request_data, render_to_template)

    chain([new_cotista_op, update_cotista_op], latest_only_join, daily_flow_report)
