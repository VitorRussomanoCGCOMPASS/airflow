from flask_api.models.funds import FundsValues, StageFundsValues
from operators.api import BritechOperator
from operators.custom_sql import MSSQLOperator, SQLCheckOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from pendulum import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 4, 14, tz="America/Sao_Paulo"),
}


def _join_ids(ids_1: None | list = None, ids_2: None | list = None):

    print(ids_1)
    print(ids_2)
    # FIXME: SOMEHOW IS COMING OUT EMPTY.

    return next(item for item in [ids_1, ids_2] if item is not None)


with DAG(
    "cotas_to_db",
    default_args=default_args,
    catchup=True,
    schedule=None,
    max_active_runs=2,
):

    is_business_day = SQLCheckOperator(
        task_id="check_for_hol",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE cast(date as date) = '{{ds}}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    latest_only = LatestOnlyOperator(
        task_id="latest_only",
    )

    chain(is_business_day, latest_only)

    with TaskGroup("latest") as latest:

        fetch_non_filled_funds = MSSQLOperator(
            task_id="fetch_non_filled_funds",
            database="DB_Brasil",
            conn_id="mssql-default",
            sql=""" 
                        SELECT cnpj FROM funds WHERE britech_id NOT IN
                        ( SELECT britech_id FROM funds_values WHERE cast(Data  as Date) = '{{macros.template_tz.convert_ts(ds)}}')
                            and closure_date >= '{{macros.template_tz.convert_ts(ds)}}' or closure_date is null
                """,
            do_xcom_push=True,
        )

        @task
        def generate_status_request(cnpjs, **context):
            from itertools import chain

            ds = context["task"].render_template(
                "{{macros.template_tz.convert_ts(ds)}}",
                context,
            )

            return list(
                map(
                    lambda cnpj: {
                        "request_params": {
                            "cnpj": cnpj[-1],
                            "data": ds,
                        },
                    },
                    chain(cnpjs),
                )
            )

        structured_status_request = generate_status_request(
            cnpjs=fetch_non_filled_funds.output
        )

        fetch_funds_status = BritechOperator.partial(
            task_id="fetch_funds_status",
            endpoint="Fromtis/ConsultaStatusCarteiras",
            echo_params=True,
            do_xcom_push=True,
        ).expand_kwargs(structured_status_request)

        @task
        def filter_open_funds(fetch_funds_status_output: list):
            #  Given the echo, we return the endpoint result along the request_params.

            from collections import ChainMap

            cnpjs_on_open = []

            for output in fetch_funds_status_output:

                output = dict(
                    ChainMap(*output)
                )  # We could simply acess the index, but this is just safer.

                if output.get("Erro") in ("1", "3"):
                    raise Exception(
                        "Could not find fund or fund does not exist : %s",
                        output.get("cnpj"),
                    )

                if output.get("Status") == "Aberto":
                    cnpjs_on_open.append(output.get("cnpj"))

            return ",".join(cnpjs_on_open)

        filtered_open_funds = filter_open_funds(fetch_funds_status.output)

        convert_cnpjs_into_ids = MSSQLOperator(
            task_id="convert_cnpjs_into_ids",
            database="DB_Brasil",
            conn_id="mssql-default",
            sql=""" SELECT britech_id from funds where cnpj in  (SELECT value FROM STRING_SPLIT('{{params.cnpjs}}' , ',' ) )""",
            do_xcom_push=True,
            parameters={"cnpjs": filtered_open_funds},
        )

        chain(
            fetch_non_filled_funds,
            structured_status_request,
            fetch_funds_status,
            filtered_open_funds,
        )

    chain(latest_only, latest)

    fetch_active_funds = MSSQLOperator(
        task_id="fetch_active_funds",
        database="DB_Brasil",
        conn_id="mssql-default",
        sql=" SELECT britech_id FROM funds  where closure_date >= '{{macros.template_tz.convert_ts(ds)}}' or closure_date is null",
        do_xcom_push=True,
        trigger_rule=TriggerRule.ALL_SKIPPED,
    )

    chain(fetch_non_filled_funds, fetch_active_funds)

    join_ids = PythonOperator(
        task_id="join_ids",
        python_callable=_join_ids,
        op_kwargs={
            "ids_1": fetch_active_funds.output,
            "ids_2": convert_cnpjs_into_ids.output,
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    @task
    def generate_cotas_request(ids: list | None = None, **context):
        from itertools import chain

        if not ids:
            raise AirflowFailException("No id provided")

        try:
            ds = context["task"].render_template(
                "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
                context,
            )
        except ValueError:
            ds = context["task"].render_template(
                "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ds),-1)}}",
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

    generated_cotas_request = generate_cotas_request(join_ids.output)

    fetch_funds_data = BritechOperator.partial(
        task_id="fetch_funds_data",
        endpoint="Fundo/BuscaHistoricoCotaDia",
        do_xcom_push=True,
    ).expand_kwargs(generated_cotas_request)

    with TaskGroup(group_id="database") as database:

        @task
        def join_data(funds_data: dict):
            result = []
            for fund_data in funds_data:
                result.append(fund_data[-1])
            return result

        pre_clean_temp_table = MSSQLOperator(
            task_id="pre_clean_temp_table",
            database="DB_Brasil",
            conn_id="mssql-default",
            sql="DELETE FROM stage_funds_values",
        )
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
                ( SELECT PLFechamento, CotaFechamento from stage_funds_values WHERE PLFechamento=0 or CotaFechamento = 0 ) 
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
                ( SELECT * from stage_funds_values WHERE cast(Data as date) != '{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}') 
                THEN 0
                ELSE 1 
                END
                """,
            database="DB_Brasil",
            conn_id="mssql-default",
        )

        string_to_date = MSSQLOperator(
            task_id="string_to_date",
            database="DB_Brasil",
            conn_id="mssql-default",
            sql=""" 
            update stage_funds_values 
                SET Data = cast(Data as date) """,
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
        generated_cotas_request,
        fetch_funds_data,
        [pre_clean_temp_table, joined_data],
        push_data,
        [check_non_zero_pl_cota, check_date],
        string_to_date,
        merge_tables,
        clean_temp_table,
    )


# TODO :  WE CAN USE MSSQL OPERATOR FOR BUSINESS DAY.
