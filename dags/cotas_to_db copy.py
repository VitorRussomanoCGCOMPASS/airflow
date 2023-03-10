from operators.britech import BritechOperator
from pendulum import datetime


from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from operators.alchemy import SQLAlchemyOperatorLocal
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


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
