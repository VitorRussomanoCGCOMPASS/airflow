from pendulum import datetime
from sensors.britech import BritechIndicesSensor
from operators.custom_wasb import BritechOperator, PostgresOperator
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup
from include.utils.is_business_day import _is_business_day
from operators.file_share import FileShareOperator
from airflow.providers.sendgrid.utils.emailer import send_email as _send_email
from include.xcom_backend import HtmlXcom

def splitdsformat(value) -> str:
    """Remove the Minutes, Seconds and miliseconds from date string.
    Eg. 2023-01-01T00:00:00 -> 2023-01-21"""
    return value.split("T")[0]


def percentformat(value) -> str:
    """Format float to str with percentage format"""

    if isinstance(value, str):
        return value
    return f"{value/100:.2%}".replace(".", ",")


def currencyformat(value) -> str:
    """Format float to str  with currency format"""

    return f"{value:09,.0f}".replace("R$-", "-R$").replace(",", ".")


def valueformat(value) -> str:
    """Format float replacing '.' with ','"""

    return f"{value:0,.8f}".replace(".", ",")


def _merge_v2(funds_data: list, complementary_data: str, filter: bool = False) -> str:

    import pandas

    data = pandas.merge(
        pandas.DataFrame(funds_data),
        pandas.read_json(complementary_data),
        left_on="IdCarteira",
        right_on="britech_id",
    )

    if filter:
        data["diff_days"] = (
            pandas.to_datetime(data["DataReferencia"])
            - pandas.to_datetime(data["inception_date"])
        ).dt.days

        data.loc[
            data.diff_days < 190,
            [
                "RentabilidadeMes",
                "RentabilidadeDia",
                "RentabilidadeAno",
                "Rentabilidade6Meses",
                "RentabilidadeInicio",
            ],
        ] = ""
    return data.to_json(orient="records")


def _render_template_v2(
    html_template: str, indices_data: list[dict], complete_funds_data: str
) -> HtmlXcom:
    from jinja2 import Environment, BaseLoader
    import json

    templateEnv = Environment(loader=BaseLoader())

    templateEnv.filters["splitdsformat"] = splitdsformat
    templateEnv.filters["percentformat"] = percentformat
    templateEnv.filters["currencyformat"] = currencyformat
    templateEnv.filters["valueformat"] = valueformat

    rtemplate = templateEnv.from_string(html_template)

    rendered_template = rtemplate.render(
        {
            "indices": indices_data,
            "funds": json.loads(complete_funds_data),
        }
    )
    return HtmlXcom(rendered_template)


def _check_for_none(input) -> bool:
    if input:
        return True
    return False


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "conn_id": "postgres",
    "database": "userdata",
    "mode": "reschedule",
    "timeout": 60 * 30,
    "max_active_runs": 1,
    "catchup": False,
    "do_xcom_push": True,
}


with DAG(
    "email_cotas_pl_vfinal",
    schedule=None,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/"],
):
    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    with TaskGroup(
        group_id="external-email-subset-funds"
    ) as external_email_subset_funds:

        fetch_indices = SQLExecuteQueryOperator(
            task_id="fetch_indices",
            sql="devops_id_text.sql",
            params={
                "dag": "email_cotas_pl",
                "task_group": "external-email-subset-funds",
                "type": "indexes",
            },
        )

        check_for_indices = ShortCircuitOperator(
            task_id="check_for_indices",
            python_callable=_check_for_none,
            op_kwargs={"input": fetch_indices.output},
            do_xcom_push=False,
        )

        fetch_funds = SQLExecuteQueryOperator(
            task_id="fetch_funds",
            sql="devops_id_text.sql",
            params={
                "dag": "email_cotas_pl",
                "task_group": "external-email-subset-funds",
                "type": "funds",
            },
        )

        check_for_funds = ShortCircuitOperator(
            task_id="check_for_funds",
            python_callable=_check_for_none,
            op_kwargs={"input": fetch_funds.output},
            do_xcom_push=False,
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
                "DataFim": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
            },
            do_xcom_push=False,
        )

        @task
        def process_xcom(ids) -> tuple[int, ...]:
            from itertools import chain

            return tuple(map(int, list(chain(*ids))[-1].split(",")))

        funds_sql_sensor = SqlSensor(
            task_id="funds_sql_sensor",
            sql=""" 
                WITH WorkTable(id) as (select britech_id from funds WHERE britech_id  = any(array{{ti.xcom_pull(task_ids='external-email-subset-funds.process_xcom')}}))
                SELECT ( SELECT COUNT(*)
                FROM funds_values
                WHERE funds_id IN ( SELECT id FROM WorkTable) 
	            AND date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}') =
	   		    (SELECT COUNT(*) FROM WorkTable)
                """,
            hook_params={"database": "userdata"},
            do_xcom_push=False,
        )  # type: ignore

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
            },
        )

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": fetch_funds.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
            },
        )

        fetch_complementary_funds_data = PostgresOperator(
            task_id="fetch_complementary_funds_data",
            conn_id="postgres",
            database="userdata",
            sql=""" 
            SELECT * FROM (WITH Worktable as (SELECT britech_id, inception_date, apelido  ,"CotaFechamento" , date , type 
            FROM funds a 
            JOIN funds_values c 
            ON a.britech_id = c.funds_id 
            WHERE britech_id = any(array{{ti.xcom_pull(task_ids='external-email-subset-funds.process_xcom')}})
            AND date = inception_date 
            OR  date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}'
            AND britech_id = any(array{{ti.xcom_pull(task_ids='external-email-subset-funds.process_xcom')}})
            )  
            , lagged as (SELECT *, LAG("CotaFechamento") OVER (PARTITION by apelido ORDER BY date) AS inception_cota
            FROM Worktable)
            SELECT britech_id , to_char(inception_date,'YYYY-MM-DD') inception_date , apelido, type,
            COALESCE(("CotaFechamento" - inception_cota)/inception_cota ) * 100 AS "RentabilidadeInicio"
            FROM lagged) as tb
            WHERE "RentabilidadeInicio" !=0
            """,
            results_to_dict=True,
        )

        merge_and_filter = PythonOperator(
            task_id="merge_and_filter",
            python_callable=_merge_v2,
            op_kwargs={
                "funds_data": fetch_funds_return.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": True,
            },
        )

        fetch_template = FileShareOperator(
            task_id="fetch_template",
            conn_id="azure_fileshare_default",
            share_name="utils",
            directory_name="Templates",
            file_name="cotas_pl_template.html",
        )

        render_template = PythonOperator(
            task_id="render_template",
            python_callable=_render_template_v2,
            op_kwargs={
                "indices_data": fetch_indices_return.output,
                "complete_funds_data": merge_and_filter.output,
                "html_template": fetch_template.output,
            },
            do_xcom_push=True,
        )

        send_email = PythonOperator(
            task_id="send_email",
            python_callable=_send_email,
            op_kwargs={
                "to": "Vitor.Ibanez@cgcompass.com",
                "subject": "CG - COMPASS GROUP INVESTIMENTOS - COTAS - {{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
                "html_content": render_template.output,
                "conn_id": "email_default",
            },
        )

        chain(
            fetch_indices,
            check_for_indices,
            indices_sensor,
            fetch_indices_return,
            render_template,
        )
        chain(
            fetch_funds,
            check_for_funds,
            process_xcom(fetch_funds.output),
            funds_sql_sensor,
            [fetch_funds_return, fetch_complementary_funds_data],
            merge_and_filter,
            render_template,
        )
        chain(fetch_template, render_template, send_email)

    chain(is_business_day, external_email_subset_funds)

    with TaskGroup(group_id="internal-email-all-funds") as all_funds:

        complete_funds_sql_sensor = SqlSensor(
            task_id="complete_funds_sql_sensor",
            sql="SELECT ( SELECT COUNT(*) FROM funds_values WHERE funds_id IN ( SELECT britech_id FROM funds WHERE status='ativo') AND date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}') = (SELECT COUNT(*) FROM funds WHERE status='ativo')",
            hook_params={"database": "userdata"},
            do_xcom_push=False,
        )  # type: ignore

        complete_fetch_funds = SQLExecuteQueryOperator(
            task_id="complete_fetch_funds",
            sql="SELECT string_agg(britech_id::text,',') as britech_id from funds where status='ativo' ",
        )

        @task
        def process_xcom(ids) -> tuple[int, ...]:
            from itertools import chain

            return tuple(map(int, list(chain(*ids))[-1].split(",")))

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": complete_fetch_funds.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
            },
        )

        fetch_complementary_funds_data = PostgresOperator(
            task_id="fetch_complementary_funds_data",
            conn_id="postgres",
            database="userdata",
            sql=""" 
            SELECT * FROM (WITH Worktable as (SELECT britech_id, inception_date, apelido  ,"CotaFechamento" , date , type 
            FROM funds a 
            JOIN funds_values c 
            ON a.britech_id = c.funds_id 
            WHERE britech_id = any(array{{ti.xcom_pull(task_ids='internal-email-all-funds.process_xcom')}})
            AND date = inception_date 
            OR date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}'
            AND britech_id = any(array{{ti.xcom_pull(task_ids='internal-email-all-funds.process_xcom')}})
            )  
            , lagged as (SELECT *, LAG("CotaFechamento") OVER (PARTITION by apelido ORDER BY date) AS inception_cota
            FROM Worktable)
            SELECT britech_id , to_char(inception_date,'YYYY-MM-DD') inception_date , apelido, type,
            COALESCE(("CotaFechamento" - inception_cota)/inception_cota ) * 100 AS "RentabilidadeInicio"
            FROM lagged) as tb
            WHERE "RentabilidadeInicio" !=0
            """,
            results_to_dict=True,
        )

        merge = PythonOperator(
            task_id="merge",
            python_callable=_merge_v2,
            op_kwargs={
                "funds_data": fetch_funds_return.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": False,
            },
        )

        fetch_template = FileShareOperator(
            task_id="fetch_template",
            conn_id="azure_fileshare_default",
            share_name="utils",
            directory_name="Templates",
            file_name="internal_cotas_template.html",
        )

        render_template = PythonOperator(
            task_id="render_template",
            python_callable=_render_template_v2,
            op_kwargs={
                "indices_data": fetch_indices_return.output,
                "complete_funds_data": merge.output,
                "html_template": fetch_template.output,
            },
            do_xcom_push=True,
        )

        send_email = PythonOperator(
            task_id="send_email",
            python_callable=_send_email,
            op_kwargs={
                "to": "Vitor.Ibanez@cgcompass.com",
                "subject": "CG - COMPASS GROUP INVESTIMENTOS - COTAS - {{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}} (INTERNAL REPORT)",
                "html_content": render_template.output,
                "conn_id": "email_default",
            },
        )

        chain(fetch_indices, indices_sensor, fetch_indices_return, render_template)

        chain(
            complete_funds_sql_sensor,
            complete_fetch_funds,
            process_xcom(complete_fetch_funds.output),
            [fetch_funds_return, fetch_complementary_funds_data],
            merge,
            render_template,
        )

        chain(fetch_template, render_template, send_email)

    chain(funds_sql_sensor, all_funds)


with DAG(
    "email_prev_cotas_pl",
    schedule=None,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/"],
):
    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    with TaskGroup(group_id="indices") as indices:

        fetch_indices = SQLExecuteQueryOperator(
            task_id="fetch_indices",
            sql="devops_id_text.sql",
            params={
                "dag": "prev_email_cotas_pl",
                "task_group": "prev_email_cotas_pl",
                "type": "indexes",
            },
        )

        check_for_indices = ShortCircuitOperator(
            task_id="check_for_indices",
            python_callable=_check_for_none,
            op_kwargs={"input": fetch_indices.output},
            do_xcom_push=False,
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
                "DataFim": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
            },
            do_xcom_push=False,
        )

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
            },
        )

        chain(fetch_indices, check_for_indices, indices_sensor, fetch_indices_return)

    with TaskGroup(group_id="funds") as funds:

        fetch_funds = SQLExecuteQueryOperator(
            task_id="fetch_funds",
            sql="devops_id_text.sql",
            params={
                "dag": "prev_email_cotas_pl",
                "task_group": "prev_email_cotas_pl",
                "type": "funds",
            },
        )

        check_for_funds = ShortCircuitOperator(
            task_id="check_for_funds",
            python_callable=_check_for_none,
            op_kwargs={"input": fetch_funds.output},
            do_xcom_push=False,
        )

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": fetch_funds.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
            },
        )

        @task
        def process_xcom(ids) -> tuple[int, ...]:
            from itertools import chain
            return tuple(map(int, list(chain(*ids))[-1].split(",")))

        fetch_complementary_funds_data = PostgresOperator(
            task_id="fetch_complementary_funds_data",
            conn_id="postgres",
            database="userdata",
            sql=""" 
                SELECT britech_id, to_char(inception_date,'YYYY-MM-DD') inception_date, apelido ,type 
                FROM funds a 
                WHERE britech_id = any(array{{ti.xcom_pull(task_ids='funds.process_xcom')}})
                """,
            results_to_dict=True,
        )

        merge = PythonOperator(
            task_id="merge",
            python_callable=_merge_v2,
            op_kwargs={
                "funds_data": fetch_funds_return.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": False,
            },
        )

        chain(
            indices_sensor,
            fetch_funds,
            check_for_funds,
            [fetch_funds_return, fetch_complementary_funds_data],
            merge,
        )
        chain(process_xcom(fetch_funds.output), fetch_complementary_funds_data)

    fetch_template = FileShareOperator(
        task_id="fetch_template",
        share_name="utils",
        conn_id="azure_fileshare_default",
        directory_name="Templates",
        file_name="prev_internal_cotas_template.html",
        do_xcom_push=True,
    )

    render_template = PythonOperator(
        task_id="render_template",
        python_callable=_render_template_v2,
        op_kwargs={
            "html_template": fetch_template.output,
            "complete_funds_data": merge.output,
            "indices_data": fetch_indices_return.output,
        },
        do_xcom_push=True,
    )

    send_email = PythonOperator(
        task_id="send_email",
        python_callable=_send_email,
        op_kwargs={
            "to": "Vitor.Ibanez@cgcompass.com",
            "subject": "CG - COMPASS GROUP INVESTIMENTOS - COTAS - {{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}} (COTAS PRÉVIAS)",
            "html_content": render_template.output,
            "conn_id": "email_default",
        },
    )

    chain(is_business_day, indices)
    chain([indices, funds], render_template)
    chain(is_business_day, fetch_template, render_template,send_email)
