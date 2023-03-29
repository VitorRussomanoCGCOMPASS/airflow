from operators.api import BritechOperator
from operators.custom_sql import MSSQLOperator
from operators.write_audit_publish import InsertSQLOperator
from pendulum import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
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

        fetch_active_funds = MSSQLOperator(
            task_id="fetch_active_funds",
            database="DB_Brasil",
            conn_id="mssql-default",
            sql="declare @date date set @date = '{{macros.template_tz.convert_ts(ds)}}' EXEC dbo.SP_ACTIVE_FUNDS @date",
            do_xcom_push=True,
        )

        @task
        def copy_kwargs(**context):
            from itertools import chain

            ids = context["task_instance"].xcom_pull(
                task_ids="funds_processing.fetch_active_funds"
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

        @task
        def join_data(funds_data: dict):
            result = []
            for fund_data in funds_data:
                result.append(fund_data[-1])
            return result

        joined_data = join_data(fetch_funds_data.output)

        push_data = InsertSQLOperator(
            task_id="push_data",
            database="DB_Brasil",
            conn_id="mssql-default",
            table=FundsValues,
            values=joined_data,
        )

        chain(
            fetch_active_funds,
            copied_kwargs,
            fetch_funds_data,
            joined_data,
            push_data,
        )
