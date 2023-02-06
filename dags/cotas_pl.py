from airflow import DAG
from pendulum import datetime
from include.utils.is_business_day import _is_business_day
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from sensors.britech import BritechIndicesSensor
from operators.britech import BritechOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from operators.extended_sql import SQLQueryToLocalOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


indices = "1 , 26 , 70 , 102, 1011"
funds = "10 , 49 , 3, 32 , 17 , 30, 42"

# COMPLETE: do_xcom false


def splitdsformat(value) -> str:
    """Remove the Minutes, Seconds and miliseconds from date string.
    Eg. 2023-01-01T00:00:00 -> 2023-01-11"""
    return value.split("T")[0]


def percentformat(value) -> str:

    """Format float to str with percentage format"""

    return f"{value/100:.2%}".replace(".", ",")


def currencyformat(value) -> str:

    """Format float to str  with currency format"""

    return f"{value:09,.0f}".replace("R$-", "-R$").replace(",", ".")


def valueformat(value) -> str:

    """Format float replacing '.' with ','"""

    return f"{value:0,.8f}".replace(".", ",")


def _render_template(
    template_path: str, output_path: str, funds_path: str, indices_path: str
) -> None:

    import jinja2
    import json

    with open(indices_path, "r") as _file:
        indices = json.load(_file)

    with open(funds_path, "r") as _file:
        funds = json.load(_file)

    templateLoader = jinja2.FileSystemLoader(searchpath=template_path)

    templateEnv = jinja2.Environment(loader=templateLoader)

    templateEnv.filters["splitdsformat"] = splitdsformat
    templateEnv.filters["percentformat"] = percentformat
    templateEnv.filters["currencyformat"] = currencyformat
    templateEnv.filters["valueformat"] = valueformat

    TEMPLATE_FILE = "cotas_pl_template.html"
    template = templateEnv.get_template(TEMPLATE_FILE)

    rendered_template = template.render(indices=indices, funds=funds)

    with open(output_path, "w+") as fh:
        fh.write(rendered_template)


with DAG(
    "email_cotas_pl_v2",
    schedule=None,
    default_args=default_args,
    catchup=False,
    template_searchpath=["/opt/airflow/include/sql/"],
    max_active_runs=1,
):

    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    with TaskGroup(group_id="subset-of-funds"):

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": indices,
                "DataInicio": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
                "DataFim": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
            mode="reschedule",
            timeout=60 * 30,
        )

        # COMPLETE: DATABASE!
        funds_subset_sql_sensor = SqlSensor(
            task_id="funds_subset_sql_sensor",
            sql="check_funds_subset.sql",
            conn_id="postgres",
            params={"ids": funds},
            hook_params={"database": "userdata"},
            mode="reschedule",
            timeout=60 * 30,
        )

        # COMPLETE : HOW CAN WE IMPLEMENT FUNDS SENSOR?
        # COMPLETE : {{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts))}}
        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": indices,
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
                "idCarteiras": funds,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            },
            output_path="/opt/airflow/data/britech/rentabilidade",
            filename="funds_{{ds}}.json",
            do_xcom_push=False,
        )

        chain(
            is_business_day,
            [funds_subset_sql_sensor, indices_sensor],
            [fetch_funds_return, fetch_indices_return],
        )

        # FIXME : IT DOESNT HAVE CONTEXT TO USE DS.
        # FIXME : IT IS NOT GETTING EVERYTHING. SMH
        fetch_complementary_funds_data = SQLQueryToLocalOperator(
            task_id="fetch_complementary_funds_data",
            file_path="/opt/airflow/data/britech/funds_comp_{{ds}}.json",
            conn_id="postgres",
            database="userdata",
            sql="comp_funds_data.sql",
            params={"ids": funds},
            do_xcom_push=False,
        )

        # TODO : APPEND FUNDS NAME , RETURN SINCE INCEPTION, INCEPTION DATE.
        render_to_template = PythonOperator(
            task_id="render_to_template",
            python_callable=_render_template,
            op_kwargs={
                "template_path": "/opt/airflow/include/templates/",
                "output_path": "/opt/airflow/data/cotas_pl_{{ds}}.html",
                "funds_path": "/opt/airflow/data/britech/rentabilidade/funds_{{ds}}.json",
                "indices_path": "/opt/airflow/data/britech/rentabilidade/indices_{{ds}}.json",
            },
        )

        send_email = EmptyOperator(task_id="send_email")

        chain(
            fetch_funds_return,
            fetch_complementary_funds_data,
        )

        chain(
            [fetch_complementary_funds_data, fetch_indices_return],
            render_to_template,
            send_email,
        )

    with TaskGroup(group_id="all-funds") as all_funds:

        funds_sql_sensor = SqlSensor(
            task_id="funds_sql_sensor",
            sql="check_funds.sql",
            conn_id="postgres",
            hook_params={"database": "userdata"},
            mode="reschedule",
            timeout=60 * 30,
        )

    chain(funds_subset_sql_sensor, all_funds)



with open