from operators.alchemy import SQLAlchemyOperatorLocal
from operators.api import BritechOperator
from pendulum import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from operators.write_audit_publish import InsertSQLOperator
from operators.custom_sql import MSSQLOperator
from flask_api.models.funds import FundsValues

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}

with DAG(
    "cotas_to_db_copy",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
):

    with TaskGroup(group_id="funds_processing") as funds_processing:

        fetch_sql_funds = MSSQLOperator(
            task_id="fetch_sql_funds",
            database="DB_Brasil",
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
                    },
                    chain(ids),
                )
            )

        copied_kwargs = copy_kwargs()

        fetch_funds_data = BritechOperator.partial(
            task_id="fetch_funds_data",
            endpoint="Fundo/BuscaHistoricoCotaDia",
            do_xcom_push=True,
        ).expand_kwargs(copied_kwargs)

        push_data = InsertSQLOperator.partial(
            task_id="push_data",
            database="DB_Brasil",
            table="funds_values",
            conn_id="mssql-default",
        ).expand(values=fetch_funds_data.output)

        chain(
            fetch_sql_funds,
            copied_kwargs,
            fetch_funds_data,
            push_data,
        )
