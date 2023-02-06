from operators.britech import BritechOperator
from pendulum import datetime


from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from plugins.task_instances import _cleanup_xcom
from operators.alchemy import SQLAlchemyOperatorLocal
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


# COMPLETE :  WE NEED TO GET IN DATABASE AND GET ALL AVAILABLE FUNDS.
# COMPLETE: EXPAND BRITECH OPERATOR.
# COMPLETE : IS DS WORKING?
# COMPLETE : IDS IN REQUEST PARAMS
# COMPLETE : FILENAME WITH DS AND ID.

funds = "10 , 49 , 3, 32 , 17 , 30, 42"


def _push_cotista_op(file_path: str, session, filename, **kwargs) -> None:
    """
    Reads json data from file specified by file_path and insert but do not update to CotistaOp table

    Parameters
    ----------
    file_path : str
    session : Session
    """
    import json
    import logging
    import os

    from flask_api.models.funds import FundsValues
    from include.schemas.funds_values import FundsValuesSchemas

    from sqlalchemy.dialects.postgresql import insert as pgs_upsert

    path = os.path.join(file_path, filename)

    with open(path, "r") as _file:
        logging.info("Getting file.")
        data = json.load(_file)

    data = FundsValuesSchemas(session=session).load(data, many=True)

    stmt = pgs_upsert(FundsValues).values(data)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["date", "funds_id"],
    )

    session.execute(stmt)


with DAG(
    "cotas_to_db_copy",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
):

    with TaskGroup(group_id="funds_processing") as funds_processing:

        fetch_sql_funds = SQLExecuteQueryOperator(
            task_id="fetch_sql_funds",
            conn_id="postgres",
            database="userdata",
            sql="SELECT britech_id from funds where status='ativo' ",
            do_xcom_push=True,
        )

        @task
        def copy_kwargs(**context):
            from itertools import chain

            ids = context["task_instance"].xcom_pull(
                task_ids="funds_processing.fetch_sql_funds"
            )
            ds = context["task"].render_template(
                "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
                context,
            )

            # FIXME: EFFICIENCY PLS.
            return list(
                map(
                    lambda id: {
                        "request_params": {
                            "idCarteira": id[-1],
                            "dataInicio": ds,
                            "dataFim": ds,
                        },
                        "filename": "fund_id" + str(id[-1]) + "_" + ds + ".json",
                    },
                    chain(ids),
                )
            )

        copied_kwargs = copy_kwargs()

        fetch_funds_data = BritechOperator.partial(
            task_id="fetch_funds_data",
            output_path="/opt/airflow/data/britech/cotas",
            endpoint="Fundo/BuscaHistoricoCotaDia",
            do_xcom_push=False,
        ).expand_kwargs(copied_kwargs)

        push_funds_data = SQLAlchemyOperatorLocal.partial(
            conn_id="postgres",
            database="userdata",
            task_id="push_cotista_op",
            python_callable=_push_cotista_op,
            file_path="/opt/airflow/data/britech/cotas",
            do_xcom_push=False,
        ).expand(op_kwargs=copied_kwargs)

        opening_new_day = EmptyOperator(task_id="opening_new_day")
        chain(
            fetch_sql_funds,
            copied_kwargs,
            fetch_funds_data,
            push_funds_data,
            opening_new_day,
        )

    cleanup_xcom = EmptyOperator(
        task_id="cleanup_xcom", on_success_callback=_cleanup_xcom
    )

    sql_branching_given_funds = BranchSQLOperator(
        task_id="sql_branching_given_funds",
        database="userdata",
        sql="check_funds.sql",
        conn_id="postgres",
        follow_task_ids_if_false=[],
        follow_task_ids_if_true=["mass_fetch"],
    )

    mass_fetch = EmptyOperator(task_id="mass_fetch")

    chain(funds_processing, cleanup_xcom, sql_branching_given_funds, mass_fetch)


# COMPLETE :  WRITE TO THE ACTUAL DATABASE.

# TODO : MARSHMALLOW VALIDATES COTAS PL TO CHECK IF NOT 0. (IMPORTANT)
# TODO : FIRST WE HAVE TO CHECK AGAINST THE DATABASE TO SEE IF IS ALREADY PROCESSED.
# TODO : MAYBE WE ERASE OUR UPLOAD IF NOT EVERYTHING WORKS
