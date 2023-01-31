from operators.alchemy import SQLAlchemyOperator
from operators.britech import BritechOperator
from operators.extended_sql import SQLQueryToLocalOperator
from pendulum import datetime
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from include.utils.is_business_day import _is_business_day
from airflow import XComArg


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

    from flask_api.models.cotista_op import CotistaOp
    from sqlalchemy.dialects.postgresql import insert as pgs_upsert

    # COMPLETE: IMPROVE PERFOMANCE. (2.0 Current release.)
    # from sqlalchemy import insert
    # session.execute(insert(CotistaOp), data)

    with open(file_path, "r") as _file:
        logging.info("Getting file.")
        data = json.load(_file)

    stmt = pgs_upsert(CotistaOp).values(data)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["IdOperacao"],
    )

    session.execute(stmt)
    logging.info("Writing Cotista Operations to database")


def _push_update_op(file_path: str, session: Session) -> None:
    """
    Reads json data from file specified by file_path and send updates to CotistaOp table

    Parameters
    ----------
    file_path : str
    session : Session
    """
    # FIXME: THIS IS UPSERTING. MEANING THAT IT IS INTRODUCING TYPE 4 INTO THE DB. SHOULD BE FILTERED BASED ON DATE & TIPO OPERACAO.
    import json
    import logging

    from flask_api.models.cotista_op import CotistaOp
    from sqlalchemy.dialects.postgresql import insert as pgs_upsert

    with open(file_path, "r") as _file:
        logging.info("Getting file")
        data = json.load(_file)

    stmt = pgs_upsert(CotistaOp).values(data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["IdOperacao"],
        set_=dict(
            ValorLiquido=stmt.excluded.ValorLiquido, ValorBruto=stmt.excluded.ValorBruto
        ),
    )
    session.execute(stmt)


def isfirst_workday(ds: str):
    """Determine first workday based on day of month and weekday (0 == Monday)"""
    import logging
    from datetime import datetime

    theday = datetime.strptime(ds, "%Y-%m-%d")
    if theday.month in (6, 12):
        if (theday.day in (2, 3) and theday.weekday() == 0) or (
            theday.day == 1 and theday.weekday() < 5
        ):
            logging.info("Calling Filter for Come cotas operation.")
            return True

    logging.info("Skpping filter for Come cotas operation.")

    return False


def _generate_update_fetch():
    file_path = "/opt/airflow/data/britech/operacoes/update.json"
    import json
    from itertools import chain

    with open(file_path) as _file:
        data = json.load(_file)

    return list(chain(*data))


# TODO : I believe it would be better if we implement 20 as come cotas.
def _filter_come_cotas():
    pass


def _fetch_update_op(file: str):
    print(file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}


with DAG(
    dag_id="email_cotista",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
):

    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    fetch_cotista_op = BritechOperator(
        task_id="fetch_cotista_op",
        output_path="/opt/airflow/data/britech/operacoes/{{ds}}.json",
        endpoint="/Distribuicao/BuscaOperacaoCotistaDistribuidor",
        request_params={
            "dtInicio": "{{ds}}",
            "dtFim": "{{ds}}",
            "cnpjCarteira": {"cnpjCarteira"},
            "idCotista": {"idCotista"},
            "tpOpCotista": {"tpOpCotista"},
            "cnpjAgente": {"cnpjAgente"},
            "tpCotista": {"tpCotista"},
        },
    )

    check_for_come_cotas = ShortCircuitOperator(
        task_id="check_for_come_cotas",
        python_callable=isfirst_workday,
        ignore_downstream_trigger_rules=False,
        provide_context=True,
    )

    filter_come_cotas = PythonOperator(
        task_id="filter_come_cotas",
        python_callable=_filter_come_cotas,
    )

    push_cotista_op = SQLAlchemyOperator(
        conn_id="postgres_userdata",
        task_id="push_cotista_op",
        python_callable=_push_cotista_op,
        op_kwargs={"file_path": "/opt/airflow/data/britech/operacoes/{{ds}}.json"},
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    chain(
        is_business_day,
        fetch_cotista_op,
        check_for_come_cotas,
        filter_come_cotas,
        push_cotista_op,
    )

    search_for_updates = SQLQueryToLocalOperator(
        task_id="search_for_updates",
        file_path="/opt/airflow/data/britech/operacoes/update.json",
        conn_id="postgres_userdata",
        sql="update_op.sql",
    )

    generate_update_fetch = PythonOperator(
        task_id="generate_update_fetch",
        python_callable=_generate_update_fetch,
    )

    fetch_update_op = PythonOperator.partial(
        task_id="fetch_update_op",
        python_callable=_fetch_update_op,
    ).expand(op_args=XComArg(generate_update_fetch))

    push_update_op = SQLAlchemyOperator(
        task_id="push_update_op",
        python_callable=_push_update_op,
        conn_id="postgres_userdata",
        op_kwargs={"file_path": "/opt/airflow/data/britech/operacoes/update.json"},
    )

    join = EmptyOperator(
        task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    chain(
        is_business_day,
        search_for_updates,
        generate_update_fetch,
        fetch_update_op,
        push_update_op,
    )

    chain([push_cotista_op, push_update_op], join)


# COMPLETE : SQL OPERATOR CANNOT FIND MOUNTED FILES.
# FIXME : PROCESS OUTPUT DOES NOT RECEIVE CONTEXT.
