from airflow import DAG
from pendulum import datetime
from operators.britech import BritechOperator
from airflow.operators.empty import EmptyOperator

default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}


with DAG("testando_post", schedule=None, default_args=default_args, catchup=False):

    data = {
        "IdIndice": 3,
        "DataReferencia": "2023-02-01T00:00:00",
        "Valor": 22.136571900,
        "TipoCotacao": "D",
    }
    header = {"Content-Type": "application/json"}

    teste = BritechOperator(
        task_id="teste",
        endpoint="/MarketData/InsereCotacaoIndice",
        method="POST",
        headers=header,
        json=data,
    )




