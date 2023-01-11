from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime


from operators.britech import BritechOperator
from utils.hol import _is_not_holiday
from airflow import DAG
from pendulum import datetime

from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator

# TODO : INSTEAD OF SENSORS. MAYBE WE CAN USE EXTERNAL TRIGGERS.


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

from airflow.utils.context import Context

# TODO : EXCHANGE FETCH FUNDS INCEPTION FOR DATABASE SEARCH!


def splitdsformat(value) -> str:
    """Remove the Minutes, Seconds and miliseconds from date string.
    Eg. 2023-01-01T00:00:00 -> 2023-01-11"""
    return value.split("T")[0]


def percentformat(value) -> str:

    """Format float to str with percentage format"""

    return f"{value:.2%}".replace(".", ",")


def currencyformat(value) -> str:

    """Format float to str  with currency format"""

    return f"{value:09,.0f}".replace("R$-", "-R$").replace(",", ".")


def valueformat(value) -> str:

    """Format float replacing '.' with ','"""

    return f"{value:0,.8f}".replace(".", ",")


import json

with open(
    r"C:\Users\Vitor Russomano\airflow\data\britech\rentabilidade\fundos_2023-01-11.json"
) as fcc_file:
    fcc_data = json.load(fcc_file)

with open(
    r"C:\Users\Vitor Russomano\airflow\data\britech\rentabilidade\indices_2023-01-11.json",
    "r",
) as fcc_file:
    indices = json.load(fcc_file)

import jinja2

templateLoader = jinja2.FileSystemLoader(
    searchpath="C:/Users/Vitor Russomano/airflow/plugins/utils/"
)

templateEnv = jinja2.Environment(loader=templateLoader)


templateEnv.filters["splitdsformat"] = splitdsformat
templateEnv.filters["percentformat"] = percentformat
templateEnv.filters["currencyformat"] = currencyformat
templateEnv.filters["valueformat"] = valueformat


TEMPLATE_FILE = "cotas_pl_template.html"
template = templateEnv.get_template(TEMPLATE_FILE)
rendered_template = template.render(indices=indices)


with open("my_new_file.html", "w") as fh:
    fh.write(rendered_template)

with DAG(
    "email_cotas_pl",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
):

    is_not_holiday = PythonOperator(
        task_id="is_not_holiday", python_callable=_is_not_holiday, provide_context=True
    )

    fetch_indices_return = BritechOperator(
        task_id="fetch_indices_return",
        endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
        data={
            "idIndices": "1 , 26 , 70 , 102 , 1011",
            "dataReferencia": "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%dT00:00:00') ) }}",
        },
        output_path="C:/Users/Vitor Russomano/airflow/data/britech/rentabilidade/indices_{{macros.ds_add(ds,-1)}}'.json",
    )

    fetch_funds_return = BritechOperator(
        task_id="fetch_funds_return",
        endpoint="/Fundo/BuscaRentabilidadeFundos",
        data={
            "idCarteiras": "10 , 49 , 3 , 32 , 17 , 30 , 42",
            "dataReferencia": "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%dT00:00:00') ) }}",
        },
        output_path="C:/Users/Vitor Russomano/airflow/data/britech/rentabilidade/fundos_{{macros.ds_add(ds,-1)}}'.json",
    )

    fetch_funds_inception = BritechOperator(
        task_id="fetch_funds_inception",
        endpoint="/Fundo/BuscaFundos",
        data={"idCliente": "10 , 49 , 3 , 32 , 17 , 30 , 42"},
        output_path="C:/Users/Vitor Russomano/airflow/data/britech/rentabilidade/cotas_{{macros.ds_add(ds,-1)}}'.json",
    )

    render_to_template = EmptyOperator(task_id="render_to_template")

    send_email = EmptyOperator(task_id="send_email")

    is_not_holiday.set_downstream([fetch_indices_return, fetch_funds_return])
    fetch_funds_return.set_downstream(fetch_funds_inception)

    # COMPLETE : GET REQUESTS FUND RETURNS
    # COMPLETE: GET REQUESTS INDICES RETURNS
    # COMPLETE : GET REQUEST FUND COTA

    # TODO : READ THE DATA
    # TODO : READ TEMPLATE AND CHANGE THE VALUES
    # TODO : SEND THE EMAIL USING SENDGRID (READING CONTACTS FROM DATABASE)
