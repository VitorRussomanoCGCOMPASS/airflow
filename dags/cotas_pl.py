import json

from operators.britech import BritechOperator
from pendulum import datetime
from include.is_not_holiday import _is_not_holiday

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}


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


def funds_append(file_path: str):
    # TODO : FUNDS MUST HAVE APELIDO AND INCEPTION_DATE
    # This will read the file, access database, append data, and save it.

    return None


def render_template(
    template_path: str, output_path: str, funds_path: str, indices_path: str
) -> None:

    import jinja2


    with open(indices_path, "r") as fcc_file:
        indices = json.load(fcc_file)

    templateLoader = jinja2.FileSystemLoader(searchpath=template_path)

    templateEnv = jinja2.Environment(loader=templateLoader)

    templateEnv.filters["splitdsformat"] = splitdsformat
    templateEnv.filters["percentformat"] = percentformat
    templateEnv.filters["currencyformat"] = currencyformat
    templateEnv.filters["valueformat"] = valueformat

    TEMPLATE_FILE = "cotas_pl_template.html"
    template = templateEnv.get_template(TEMPLATE_FILE)

    rendered_template = template.render(indices=indices)  # funds = funds
    with open(output_path, "w") as fh:
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
        output_path="/opt/airflow/data/britech/rentabilidade/indices_'{{macros.ds_add(ds,-1)}}'.json",
    )

    fetch_funds_return = BritechOperator(
        task_id="fetch_funds_return",
        endpoint="/Fundo/BuscaRentabilidadeFundos",
        data={
            "idCarteiras": "10 , 49 , 3 , 32 , 17 , 30 , 42",
            "dataReferencia": "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%dT00:00:00') ) }}",
        },
        output_path="/opt/airflow/data/britech/rentabilidade/funds_{{macros.ds_add(ds,-1)}}.json",
    )

    db_fund_info = PythonOperator(
        task_id="db_fund_info",
        python_callable=funds_append,
        provide_context=True,
        op_kwargs={
            "file_path": "/opt/airflow/data/britech/rentabilidade/funds_{{macros.ds_add(ds,-1)}}.json"
        },
    )

    render_to_template = PythonOperator(
        task_id="render_to_template",
        python_callable=render_template,
        op_kwargs={
            "template_path": "/opt/airflow/include/",
            "output_path": "/opt/airflow/data/mail-template/{{ds}}.html",
            "funds_path": "/opt/airflow/data/britech/rentabilidade/funds_{{ds}}.html",
            "indices_path": "/opt/airflow/data/britech/rentabilidade/indices_{{ds}}.html",
        },
        provide_context=True,
    )

    send_email = EmptyOperator(task_id="send_email")

    is_not_holiday.set_downstream([fetch_indices_return, fetch_funds_return])
    fetch_funds_return.set_downstream(db_fund_info)
    chain([fetch_indices_return, db_fund_info], render_to_template, send_email)

    # COMPLETE : GET REQUESTS FUND RETURNS
    # COMPLETE: GET REQUESTS INDICES RETURNS

    # COMPLETE: READ THE DATA
    # COMPLETE: READ TEMPLATE AND CHANGE THE VALUES
    # TODO : READ DATABASE AND GET EXTRA INFO
    # TODO : SEND THE EMAIL USING SENDGRID (READING CONTACTS FROM DATABASE
