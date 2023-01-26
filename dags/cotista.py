
from operators.britech import BritechOperator
from pendulum import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sendgrid.utils.emailer import send_email
from include.utils.is_business_day import _is_business_day
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import chain




def isfirst_workday(ds: str):
    """Determine first workday based on day of month and weekday (0 == Monday)"""
    from datetime import datetime
    
    theday = datetime.strptime(ds, "%Y-%m-%d")
    if theday.month in (6, 12):
        if (theday.day in (2, 3) and theday.weekday() == 0) or (
            theday.day == 1 and theday.weekday() < 5
        ):
            return True

    return False 


def filter(ds : str):
    pass
    # TODO : FILTER



default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}


with DAG(
    dag_id="email_cotista",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
):

    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    fetch_cotista_op = BritechOperator(
        task_id="fetch_cotista_op",
        output_path="/opt/airflow/data/britech/operacoes/{{ds}}.json",
        endpoint="/Distribuicao/BuscaOperacaoCotistaDistribuidor",
        request_params={
            "dtInicio": "{{macros.ds_format(ds,'%Y-%m-%d','%Y-%m-%dT:%H:%M:%S')}}",
            "dtFim": "{{macros.ds_format(ds,'%Y-%m-%d','%Y-%m-%dT:%H:%M:%S')}}",
            "cnpjCarteira": {"cnpjCarteira"},
            "idCotista": {"idCotista"},
            "tpOpCotista": {"tpOpCotista"},
            "cnpjAgente": {"cnpjAgente"},
            "tpCotista": {"tpCotista"},
        },
    )

    check_for_come_cotas = ShortCircuitOperator(
        task_id="check_for_come_cotas",
        python_callable=isfirst_workday,
        ignore_downstream_trigger_rules=False,
        provide_context=True,
    )

    cc_filter = PythonOperator(task_id="filter", python_callable=filter)

    teste = EmptyOperator(task_id="teste", trigger_rule=TriggerRule.NONE_FAILED)

    chain(is_business_day, fetch_cotista_op, check_for_come_cotas, cc_filter, teste)





""" 
data = pandas.read_json("data/britech/operacoes/teste.json")
df = (
    data.groupby(["DataOperacao", "TipoOperacao", "ApelidoCarteira"])["ValorLiquido"]
    .sum()
    .reset_index()
)
df.TipoOperacao.replace({1: 1, 5: -1}, inplace=True)
df[["TipoOperacao", "ValorLiquido"]].cumprod(axis=1)
 """




import pandas
data = pandas.read_json('data/britech/operacoes/2023-01-26.json')

df = (
    data.groupby(["DataOperacao", "TipoOperacao", "ApelidoCarteira"])["ValorLiquido"]
    .sum()
    .reset_index()
)

df.TipoOperacao.replace({1: 1, 5: -1}, inplace=True)



# TODO : SE FOR 101 OR 4 TEMOS QUE PEGAR A ULTIMA COTA DISPONIVEL.



""" 


1 : Aplicacao
101 : Aplicacao Cotas


5 : Resgate Total
2 : Resgate Bruto
4 : Resgate Cotas



Considera 1 , 2 e 5 se nao for do feeder

4 Não deve ser considerado nos dias de come cota

primeiro de dezembro e primeiro de junho (util)



WORRY ABOUT FEEDERS!


 """