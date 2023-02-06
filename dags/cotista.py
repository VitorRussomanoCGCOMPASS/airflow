from operators.alchemy import SQLAlchemyOperator
from operators.britech import BritechOperator
from pendulum import datetime
from sensors.britech import BritechEmptySensor
from sqlalchemy.orm import Session

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

# FIXME : do_xcom_false
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
* latest_only: LatestOnlyOperator that checks if its the latest run. Else does not run update branch.
* look_for_updates: BranchSqlOperator that checks if any valid operations require update
* update_cotista_op: Runs a SQL Query that updates **ValorLiquido** and **Bruto** to the **Quantity** x **ValorCota** of the **DataConversao**


## Email
When all the updated data and daily operations data are available in the database. We can move forward on sending the daily email.
* request_data : Requests all the data and make all possible transformations inside SQL and save to a file.
* send_email
"""

# COMPLETE: IMPROVE PERFOMANCE. (2.0 Current release.)
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


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


with DAG(
    dag_id="cotista_operations_v2",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    doc_md=doc_md_DAG,
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
            "dtInicio": "{{macros.template_tz.convert_ts(ts)}}",
            "dtFim": "{{macros.template_tz.convert_ts(ts)}}",
            "cnpjCarteira": {"cnpjCarteira"},
            "idCotista": {"idCotista"},
            "tpOpCotista": {"tpOpCotista"},
            "cnpjAgente": {"cnpjAgente"},
            "tpCotista": {"tpCotista"},
        },
        do_xcom_push=False,
    )

    # COMPLETE : RECEIVE CONNECTION ID AND DATABASE .

    push_cotista_op = SQLAlchemyOperator(
        conn_id="postgres",
        database="userdata",
        task_id="push_cotista_op",
        python_callable=_push_cotista_op,
        op_kwargs={"file_path": "/opt/airflow/data/britech/operacoes/ds.json"},
        depends_on_past=True,
    )

    latest_only_join = LatestOnlyOperator(
        task_id="latest_only_join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    chain(
        is_business_day,
        cotista_op_sensor,
        fetch_cotista_op,
        push_cotista_op,
        latest_only_join,
    )

    latest_only = LatestOnlyOperator(task_id="latest_only")

    # COMPLETE : CHANGE POSTGRES USER DATA TO POSTGRES AND THEN DATABASE IS USERDATA.
    look_for_updates = BranchSQLOperator(
        task_id="look_for_updates",
        conn_id="postgres",
        database="userdata",
        sql="op_check_for_update.sql",
        follow_task_ids_if_false=["latest_only_join"],
        follow_task_ids_if_true=["update_cotista_op"],
    )

    # COMPLETE: SQL
    update_cotista_op = SQLExecuteQueryOperator(
        task_id="update_cotista_op",
        sql="op_update.sql",
        conn_id="postgres",
        database="userdata",
        
    )

    chain(
        is_business_day,
        latest_only,
        look_for_updates,
        update_cotista_op,
        latest_only_join,
    )

    check_data = SQLCheckOperator(
        task_id="check_data",
        conn_id="postgres",
        database="userdata",
        sql="check_funds.sql"
    )
    
    request_data = SQLQueryToLocalOperator(
        task_id="request_data",
        file_path="/opt/airflow/data/cotista_op_{{ds}}.json",
        conn_id="postgres",
        database="userdata",
        sql="",
    )

    render_to_template = EmptyOperator(task_id="render_to_template")

    chain(latest_only_join,check_data, request_data, render_to_template)

# COMPLETE : SQL OPERATOR CANNOT FIND MOUNTED FILES.
