from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
from airflow.providers.sendgrid.utils.emailer import send_email


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}



HtmlFile = open(r'/opt/airflow/data/all_cotas_pl_2023-02-24.html', 'r', encoding='utf-8')
source_code = HtmlFile.read()

with DAG(
    "example_dag",
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
) as dag:

    ok = PythonOperator(
        task_id="ok",
        python_callable=send_email,
        op_kwargs={
            "to": "Vitor.Ibanez@cgcompass.com",
            "subject": "CG - COMPASS GROUP INVESTIMENTOS - COTAS - {{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}",
            "html_content": source_code,
            "conn_id": "email_default",
        },
    )

