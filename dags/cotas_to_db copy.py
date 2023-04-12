from flask_api.models.funds import FundsValues, StageFundsValues
from operators.api import BritechOperator
from operators.custom_sql import MSSQLOperator, SQLCheckOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from pendulum import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


with DAG(
    "funds_values_db_dev",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
):

    with TaskGroup(group_id="funds_processing") as funds_processing:

        pre_clean_temp_table = MSSQLOperator(
            task_id="pre_clean_temp_table",
            database="DB_Brasil",
            conn_id="mssql-default",
            sql="DELETE FROM stage_funds_values",
        )

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
            table=StageFundsValues,
            values=joined_data,
        )

        check_non_zero_pl_cota = SQLCheckOperator(
            task_id="check_non_zero_pl_cota",
            sql="""
                SELECT CASE WHEN 
                    NOT EXISTS 
                ( SELECT PLFechamento, CotaFechamento from stage_funds_values where PLFechamento=0 or CotaFechamento = 0 ) 
                THEN 1 
                ELSE 0 
                END
                """,
            database="DB_Brasil",
            conn_id="mssql-default",
        )

        check_date = SQLCheckOperator(
            task_id="check_date",
            sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_funds_values where cast(Data as date) != '{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}') 
                THEN 0
                ELSE 1 
                END
                """,
            database="DB_Brasil",
            conn_id="mssql-default",
        )

        merge_tables = MergeSQLOperator(
            task_id="merge_tables",
            source_table=StageFundsValues,
            target_table=FundsValues,
            holdlock=True,
            database="DB_Brasil",
            conn_id="mssql-default",
            set_=("PLFechamento", "CotaFechamento"),
        )

        clean_temp_table = MSSQLOperator(
            task_id="clean_temp_table",
            database="DB_Brasil",
            conn_id="mssql-default",
            sql="DELETE FROM stage_funds_values",
        )

        chain(
            pre_clean_temp_table,
            fetch_active_funds,
            copied_kwargs,
            fetch_funds_data,
            joined_data,
            push_data,
            [check_non_zero_pl_cota, check_date],
            merge_tables,
            clean_temp_table,
        )
