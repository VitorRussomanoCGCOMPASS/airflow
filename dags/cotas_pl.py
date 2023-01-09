from hooks.britech import BritechHook
from operators.britech import BritechOperator

from airflow import DAG
from pendulum import datetime


default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}

with DAG("cotas_pl", schedule="@daily", default_args=default_args, catchup=False):

    fetch_indices = BritechOperator(
        endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
        data={
            "idIndices": "1 , 26 , 70 , 102 , 1011",
            "dataReferencia": "2023-01-04T00:00:00",
        },
        output_path="str",
    )

    fetch_fundos = BritechOperator(
        endpoint="/Fundo/BuscaRentabilidadeFundos",
        data={
            "idCarteiras": "10 , 49 , 3 , 32 , 17 , 30 , 42",
            "dataReferencia": "2023-01-04T00:00:00",
        },
        output_path="str",
    )


# COMPLETE : CREATE OPERATOR
# TODO : AIRFLOW VARIABLES


# COMPLETE : 1011 DOES NOT HAVE DATA

BritechHook().run(
    endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
    data={
        "idIndices": "1 , 26 , 70 , 102 , 1011",
        "dataReferencia": "2023-01-04T00:00:00",
    },
)


# https://saas.britech.com.br/compass_ws/Help/Api/GET-api-Fundo-BuscaFundos_idCliente



BritechHook().run(
    endpoint="/Fundo/BuscaRentabilidadeFundos",
    data={
        "idCarteiras": "10 , 49 , 3 , 32 , 17 , 30 , 42",
        "dataReferencia": "2023-01-04T00:00:00",
    },
)
# TODO : WHAT WILL TRIGGER ANOTHER SENSOR POKE?

# TODO : REMOVE FILE COLUMNS THAT WE DONT HAVE YET
# TODO : TEMPLATE SUB
