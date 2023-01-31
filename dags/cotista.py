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
from sensors.britech import BritechEmptySensor

doc_md_DAG = """ 
# Cotista Operations

This process is responsible for pushing new operations into the database. And pushing new data (**ValorLiquido** and **ValorBruto**) to old operations. Also, sending and e-mail containing MTD, YTD and Daily Flows.

## New Data
* sensor_cotista_op : Britech sensor responsible to watch for the daily new operation data.
* fetch_cotista_op: Britech operator responbile to fetch the available daily new operations data.
* check_for_come_cotas / filter_come_cotas: Given that we do not use 20 to indicate come cotas. In days such as 01/06 and 01/12 we have to filter all 4 operations so we don't send to database.
* push_cotista_op: We send to the database all new daily operation data.


## Updates
We need a way to retroactive look into old operations that have not yet been 'cotizadas'. 

* search_for_updates: Looks into the database and gets all the operations that will be 'cotizadas', meaning that requires an update.
* fetch_update_op : Fetches from britech the new values respective to the **DataOperacao** of each operation in need of an update. //Note. We are required to request for the whole day of the operation since there is no way of require a singular operation.
* filter_come_cotas_update : Since we do not use 20 to indicate come cotas. Here we have to filter based on **DataOperacao** and **TipoOperacao**.
* generate_files_to_open : Generates all the files that should be updated to the database.
* push_update_op: Send those files to the database.

## Email
When all the updated data and daily operations data are available in the database. We can move forward on sending the daily email.
* request_data : Requests all the data and make all possible transformations inside SQL and save to a file.
* send_email
"""


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


def _push_update_op(file, *, path: str, session: Session) -> None:
    """
    Reads json data from file specified by file_path and
    updates ValorLiquido and ValorBruto to CotistaOp table

    Parameters
    ----------
    file_path : str
    session : Session
    """
    # COMPLETE: SHOULD READ UPDATE.jSON AND GET ALL THE FILENAMES.
    # FIXME: THIS IS UPSERTING. MEANING THAT IT IS INTRODUCING TYPE 4 INTO THE DB. SHOULD BE FILTERED BASED ON DATE & TIPO OPERACAO.

    import json
    import logging
    import os
    from flask_api.models.cotista_op import CotistaOp
    from sqlalchemy.dialects.postgresql import insert as pgs_upsert

    file_path = os.path.join(path, file)

    with open(file_path, "r") as _file:
        logging.info("Getting file %s", file_path)
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


def _generate_update_fetch(file_path: str):
    import json
    from itertools import chain

    with open(file_path) as _file:
        data = json.load(_file)

    new_data = [
        {
            "request_params": {
                "dtInicio": i[0],
                "dtFim": i[0],
                "cnpjCarteira": "",
                "idCotista": "",
                "tpOpCotista": "",
                "cnpjAgente": "",
                "tpCotista": "",
            },
            "filename": "update_" + i[0] + ".json",
        }
        for i in list(chain(*data))
    ]

    return new_data


def _generate_files_to_open(file_path: str):
    import json
    from itertools import chain

    with open(file_path) as _file:
        data = json.load(_file)

    new_data = [["update_" + i[0] + ".json"] for i in list(chain(*data))]
    return new_data


# TODO : I believe it would be better if we implement 20 as come cotas.
def _filter_come_cotas():
    pass


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}


with DAG(
    dag_id="cotista_operations",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    doc_md = doc_md_DAG
):

    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    cotista_op_sensor = BritechEmptySensor(
        task_id="sensor_cotista_op",
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
        mode="reschedule",
        timeout=60 * 30,
    )

    fetch_cotista_op = BritechOperator(
        task_id="fetch_cotista_op",
        output_path="/opt/airflow/data/britech/operacoes",
        filename="{{ds}}.json",
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
        do_xcom_push=False,
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
        depends_on_past=True,
    )

    chain(
        is_business_day,
        cotista_op_sensor,
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
        op_kwargs={"file_path": "data/britech/operacoes/update.json"},
    )

    fetch_update_op = BritechOperator.partial(
        task_id="fetch_update_op",
        output_path="/opt/airflow/data/britech/operacoes",
        endpoint="/Distribuicao/BuscaOperacaoCotistaDistribuidor",
        do_xcom_push=False,
    ).expand_kwargs(XComArg(generate_update_fetch))

    generate_files_to_open = PythonOperator(
        task_id="generate_files_to_open",
        python_callable=_generate_files_to_open,
        op_kwargs={"file_path": "data/britech/operacoes/update.json"},
    )

    push_update_op = SQLAlchemyOperator.partial(
        task_id="push_update_op",
        python_callable=_push_update_op,
        conn_id="postgres_userdata",
        op_kwargs={"path": "/opt/airflow/data/britech/operacoes/"},
    ).expand(op_args=XComArg(generate_files_to_open))

    filter_come_cotas_update = PythonOperator(
        task_id="filter_come_cotas_update",
        python_callable=_filter_come_cotas,
    )

    join = EmptyOperator(
        task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    chain(
        is_business_day,
        search_for_updates,
        generate_update_fetch,
        fetch_update_op,
        filter_come_cotas_update,
        generate_files_to_open,
        push_update_op,
    )

    chain([push_cotista_op, push_update_op], join)


# COMPLETE : SQL OPERATOR CANNOT FIND MOUNTED FILES.
# FIXME : PROCESS OUTPUT DOES NOT RECEIVE CONTEXT.
