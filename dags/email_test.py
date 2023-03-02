from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
import logging
from airflow.operators.empty import EmptyOperator
from airflow.providers.sendgrid.utils.emailer import send_email



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


def _render_template(
    template_path: str,
    template_file: str,
    output_path: str,
    indices_path: str,
    funds_path: str,
) -> str:
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

    return rendered_template

def print_the(input):
    print(input)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": "Vitor.Ibanez@cgcompass.com",
}
with DAG(
    "email_test",
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
) as dag:

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
        do_xcom_push=True,
    )

    ok = PythonOperator(
        task_id="ok",
        python_callable=send_email,
        op_kwargs={
            "to": "Vitor.Ibanez@cgcompass.com",
            "subject": "TESTANDO",
            "html_content": render_template.output,
            "conn_id": "email_default",
        },
    )

