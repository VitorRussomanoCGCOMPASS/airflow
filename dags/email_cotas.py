import logging

from operators.britech import BritechOperator
from operators.extended_sql import SQLQueryToLocalOperator
from pendulum import datetime
from sensors.britech import BritechIndicesSensor

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup
from include.utils.is_business_day import _is_business_day

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


def _merge(output_path: str, funds_path: str, comp_path: str, filter: bool) -> None:
    """
    Merges files and optionally filters.
    (If the fund has less than 190 days we shouldn't send the rentabilidade metrics)


    Parameters
    ----------
    output_path : str
        _description_
    funds_path : str
        _description_
    comp_path : str
        _description_
    filter : bool
        _description_
    """

    import pandas

    funds = pandas.read_json(funds_path)
    comp = pandas.read_json(comp_path)
    data = pandas.merge(funds, comp, left_on="IdCarteira", right_on="britech_id")

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

    data.to_json(output_path, orient="records")


def _render_template(
    template_path: str,
    template_file: str,
    output_path: str,
    indices_path: str,
    funds_path: str,
) -> None:
    """

    Renders indices and funds data to the html template.

    Parameters
    ----------
    template_path : str
        HTML template path

    output_path : str

    indices_path : str

    funds_path : str

    """
    import json

    import jinja2

    templateLoader = jinja2.FileSystemLoader(searchpath=template_path)

    templateEnv = jinja2.Environment(loader=templateLoader)

    templateEnv.filters["splitdsformat"] = splitdsformat
    templateEnv.filters["percentformat"] = percentformat
    templateEnv.filters["currencyformat"] = currencyformat
    templateEnv.filters["valueformat"] = valueformat

    template = templateEnv.get_template(template_file)

    try:
        with open(indices_path, "r") as _file:
            indices = json.load(_file)
        with open(funds_path, "r") as _file:
            funds = json.load(_file)

    except FileNotFoundError:
        logging.error("Could not find Indices or Funds file")
        raise

    rendered_template = template.render({"indices": indices, "funds": funds})

    with open(output_path, "w+") as fh:
        fh.write(rendered_template)


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

    with TaskGroup(group_id="external-email-subset-funds") as subset_funds:

        fetch_indices = SQLExecuteQueryOperator(
            task_id="fetch_indices",
            sql="devops_id_text.sql",
            params={
                "dag": "email_cotas_pl",
                "task_group": "external-email-subset-funds",
                "type": "indexes",
            },
            do_xcom_push=True,
        )

        check_for_indices = ShortCircuitOperator(
            task_id="check_for_indices",
            python_callable=_check_for_none,
            op_kwargs={"input": fetch_indices.output},
        )

        fetch_funds = SQLExecuteQueryOperator(
            task_id="fetch_funds",
            sql="devops_id_text.sql",
            params={
                "dag": "email_cotas_pl",
                "task_group": "external-email-subset-funds",
                "type": "funds",
            },
            do_xcom_push=True,
        )

        check_for_funds = ShortCircuitOperator(
            task_id="check_for_funds",
            python_callable=_check_for_none,
            op_kwargs={"input": fetch_funds.output},
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
                "DataFim": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
        )

        @task
        def process_xcom(ids):
            from itertools import chain
            return tuple(map(int, list(chain(*ids))[-1].split(",")))

        funds_sql_sensor = SqlSensor(
            task_id="funds_sql_sensor",
            sql=""" 
                WITH WorkTable(id) as (select britech_id from funds WHERE britech_id  = any(array{{ti.xcom_pull(task_ids='external-email-subset-funds.process_xcom')}}))
                SELECT ( SELECT COUNT(*)
                FROM funds_values
                WHERE funds_id IN ( SELECT id FROM WorkTable) 
	            AND date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}') =
	   		    (SELECT COUNT(*) FROM WorkTable)
                """,
            hook_params={"database": "userdata"},
        )  # type: ignore

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
            output_path="/opt/airflow/data/britech/rentabilidade",
            filename="indices_{{ds}}.json",
            do_xcom_push=False,
        )

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": fetch_funds.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
            output_path="/opt/airflow/data/britech/rentabilidade",
            filename="funds_{{ds}}.json",
            do_xcom_push=False,
        )

        fetch_complementary_funds_data = SQLQueryToLocalOperator(
            task_id="fetch_complementary_funds_data",
            file_path="/opt/airflow/data/britech/funds_comp.json",
            conn_id="postgres",
            database="userdata",
            sql=""" 
            SELECT * FROM (WITH Worktable as (SELECT britech_id, inception_date, apelido  ,"CotaFechamento" , date , type 
            FROM funds a 
            JOIN funds_values c 
            ON a.britech_id = c.funds_id 
            WHERE britech_id = any(array{{ti.xcom_pull(task_ids='external-email-subset-funds.process_xcom')}})
            AND date = inception_date 
            OR  date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}'
            AND britech_id = any(array{{ti.xcom_pull(task_ids='external-email-subset-funds.process_xcom')}})
            )  
            , lagged as (SELECT *, LAG("CotaFechamento") OVER (PARTITION by apelido ORDER BY date) AS inception_cota
            FROM Worktable)
            SELECT britech_id , to_char(inception_date,'YYYY-MM-DD') inception_date , apelido, type,
            COALESCE(("CotaFechamento" - inception_cota)/inception_cota ) * 100 AS "RentabilidadeInicio"
            FROM lagged) as tb
            WHERE "RentabilidadeInicio" !=0
            """,
            do_xcom_push=True,
        )

        merge_and_filter = PythonOperator(
            task_id="merge_and_filter",
            python_callable=_merge,
            op_kwargs={
                "output_path": "/opt/airflow/data/funds_final_{{ds}}.json",
                "funds_path": "/opt/airflow/data/britech/rentabilidade/funds_{{ds}}.json",
                "comp_path": "/opt/airflow/data/britech/funds_comp.json",
                "filter": True,
            },
        )

        render_template = PythonOperator(
            task_id="render_template",
            python_callable=_render_template,
            op_kwargs={
                "template_path": "/opt/airflow/include/templates/",
                "template_file": "cotas_pl_template.html",
                "output_path": "/opt/airflow/data/cotas_pl_{{ds}}.html",
                "funds_path": "/opt/airflow/data/funds_final_{{ds}}.json",
                "indices_path": "/opt/airflow/data/britech/rentabilidade/indices_{{ds}}.json",
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

    chain(is_business_day, subset_funds)

    with TaskGroup(group_id="internal-email-all-funds") as all_funds:
        complete_funds_sql_sensor = SqlSensor(
            task_id="complete_funds_sql_sensor",
            sql="check_funds.sql",
            hook_params={"database": "userdata"},
            do_xcom_push=True,
        )  # type: ignore

        complete_fetch_funds = SQLExecuteQueryOperator(
            task_id="complete_fetch_funds",
            sql="SELECT string_agg(britech_id::text,',') as britech_id from funds where status='ativo' ",
            do_xcom_push=True,
        )

        @task
        def process_xcom(ids):
            from itertools import chain
            return tuple(map(int, list(chain(*ids))[-1].split(",")))

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": complete_fetch_funds.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
            output_path="/opt/airflow/data/britech/rentabilidade",
            filename="all_funds_{{ds}}.json",
            do_xcom_push=False,
        )

        fetch_complementary_funds_data = SQLQueryToLocalOperator(
            task_id="fetch_complementary_funds_data",
            file_path="/opt/airflow/data/britech/all_funds_comp.json",
            conn_id="postgres",
            database="userdata",
            sql=""" 
            SELECT * FROM (WITH Worktable as (SELECT britech_id, inception_date, apelido  ,"CotaFechamento" , date , type 
            FROM funds a 
            JOIN funds_values c 
            ON a.britech_id = c.funds_id 
            WHERE britech_id = any(array{{ti.xcom_pull(task_ids='internal-email-all-funds.process_xcom')}})
            AND date = inception_date 
            OR date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}'
            AND britech_id = any(array{{ti.xcom_pull(task_ids='internal-email-all-funds.process_xcom')}})
            )  
            , lagged as (SELECT *, LAG("CotaFechamento") OVER (PARTITION by apelido ORDER BY date) AS inception_cota
            FROM Worktable)
            SELECT britech_id , to_char(inception_date,'YYYY-MM-DD') inception_date , apelido, type,
            COALESCE(("CotaFechamento" - inception_cota)/inception_cota ) * 100 AS "RentabilidadeInicio"
            FROM lagged) as tb
            WHERE "RentabilidadeInicio" !=0
            """,
            do_xcom_push=True,
        )

        merge = PythonOperator(
            task_id="merge",
            python_callable=_merge,
            op_kwargs={
                "output_path": "/opt/airflow/data/all_funds_final_{{ds}}.json",
                "funds_path": "/opt/airflow/data/britech/rentabilidade/all_funds_{{ds}}.json",
                "comp_path": "/opt/airflow/data/britech/all_funds_comp.json",
                "filter": False,
            },
        )

        render_template = PythonOperator(
            task_id="render_template",
            python_callable=_render_template,
            op_kwargs={
                "template_path": "/opt/airflow/include/templates/",
                "template_file": "internal_cotas_template.html",
                "output_path": "/opt/airflow/data/all_cotas_pl_{{ds}}.html",
                "funds_path": "/opt/airflow/data/all_funds_final_{{ds}}.json",
                "indices_path": "/opt/airflow/data/britech/rentabilidade/indices_{{ds}}.json",
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
            do_xcom_push=True,
        )

        check_for_indices = ShortCircuitOperator(
            task_id="check_for_indices",
            python_callable=_check_for_none,
            op_kwargs={"input": fetch_indices.output},
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
                "DataFim": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
        )

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
            output_path="/opt/airflow/data/britech/rentabilidade",
            filename="indices_{{ds}}.json",
            do_xcom_push=False,
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
            do_xcom_push=True,
        )

        check_for_funds = ShortCircuitOperator(
            task_id="check_for_funds",
            python_callable=_check_for_none,
            op_kwargs={"input": fetch_funds.output},
        )

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": fetch_funds.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
            output_path="/opt/airflow/data/britech/rentabilidade",
            filename="prev_funds_{{ds}}.json",
            do_xcom_push=False,
        )

        @task
        def process_xcom(ids):
            from itertools import chain
            return tuple(map(int, list(chain(*ids))[-1].split(",")))

        fetch_complementary_funds_data = SQLQueryToLocalOperator(
            task_id="fetch_complementary_funds_data",
            file_path="/opt/airflow/data/britech/prev_funds_comp.json",
            conn_id="postgres",
            database="userdata",
            sql=""" 
                SELECT britech_id, to_char(inception_date,'YYYY-MM-DD') inception_date, apelido ,type 
                FROM funds a 
                WHERE britech_id = any(array{{ti.xcom_pull(task_ids='funds.process_xcom')}})
                """,
            do_xcom_push=True,
        )

        merge = PythonOperator(
            task_id="merge",
            python_callable=_merge,
            op_kwargs={
                "output_path": "/opt/airflow/data/prev_funds_final_{{ds}}.json",
                "funds_path": "/opt/airflow/data/britech/rentabilidade/prev_funds_{{ds}}.json",
                "comp_path": "/opt/airflow/data/britech/prev_funds_comp.json",
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

    render_template = PythonOperator(
        task_id="render_template",
        python_callable=_render_template,
        op_kwargs={
            "template_path": "/opt/airflow/include/templates/",
            "template_file": "prev_internal_cotas_template.html",
            "output_path": "/opt/airflow/data/prev_cotas_pl_{{ds}}.html",
            "funds_path": "/opt/airflow/data/prev_funds_final_{{ds}}.json",
            "indices_path": "/opt/airflow/data/britech/rentabilidade/indices_{{ds}}.json",
        },
    )

    chain(is_business_day, indices)
    chain([indices,funds],render_template)




