from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}



# THIS WILL BE TRIGERRED BY TWO PROCESSES

with DAG("email_cotas_pl", schedule="@daily", default_args=default_args,max_active_runs=1, catchup=False):
    pass
    # TODO : GET REQUESTS FUND RETURNS
    # TODO : GET REQUESTS INDICES RETURNS
    # TODO : GET REQUEST FUND COTA

    
    # TODO : READ TEMPLATE AND CHANGE THE VALUES
    # TODO : SEND THE EMAIL USING SENDGRID
