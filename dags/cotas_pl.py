from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime


from operators.britech import BritechOperator
from utils.hol import _is_not_holiday
from airflow import DAG
from pendulum import datetime

from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator
# TODO : WRITE THE SENSORS. THAT WAY WE KEEP THE TASKS ATOMIC.

default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1),}

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
            "dataReferencia": "2023-01-04T00:00:00",
        },
        output_path="str",
    )

    fetch_funds_return = BritechOperator(
        task_id="fetch_funds_return",
        endpoint="/Fundo/BuscaRentabilidadeFundos",
        data={
            "idCarteiras": "10 , 49 , 3 , 32 , 17 , 30 , 42",
            "dataReferencia": "2023-01-04T00:00:00",
        },
        output_path="str",
    )

    fetch_funds_cota = BritechOperator(
        task_id="fetch_funds_cota",
        endpoint="/Fundo/BuscaFundos",
        data={"idCliente": "10 , 49 , 3 , 32 , 17 , 30 , 42"},
        output_path="str",
    )

    read_template = PythonOperator(
        task_id = 'read_template',python_callable=template_reader
    )

    is_not_holiday.set_downstream([fetch_indices_return, fetch_funds_return])
    fetch_funds_return.set_downstream(fetch_funds_cota)





    # COMPLETE : GET REQUESTS FUND RETURNS
    # COMPLETE: GET REQUESTS INDICES RETURNS
    # COMPLETE : GET REQUEST FUND COTA

    # TODO : READ THE DATA
    # TODO : READ TEMPLATE AND CHANGE THE VALUES
    # TODO : SEND THE EMAIL USING SENDGRID
