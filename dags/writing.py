import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime

import logging 

def write_to_data():




    from airflow.hooks.base import BaseHook

    bima = BaseHook().get_connection('anbima_api')
    logging.info(bima.conn_id)



with DAG(
    "wrintg_to_bind", schedule="@daily", start_date=datetime(2023, 1, 1), catchup=False
):
    data = PythonOperator(task_id="data", python_callable=write_to_data)

