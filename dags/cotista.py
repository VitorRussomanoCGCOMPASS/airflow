from operators.britech import BritechOperator
from pendulum import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from include.utils.is_business_day import _is_business_day
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import chain
from operators.alchemy import SQLAlchemyOperator
from sqlalchemy.orm import Session


def _push_cotista_op(file_path: str, session: Session):
    import json
    from include.schemas.cotista_op import CotistaOpSchema
    import logging 
    
    with open(file_path, "r") as _file:
        logging.info('Getting file.')
        data = json.load(_file)

    cotista_op_objs = CotistaOpSchema(session=session).load(data,many=True)
    session.add_all(cotista_op_objs)
    logging.info('Writing Cotista Operations to database')



def isfirst_workday(ds: str):
    """Determine first workday based on day of month and weekday (0 == Monday)"""
    from datetime import datetime
    import logging 
    
    theday = datetime.strptime(ds, "%Y-%m-%d")
    if theday.month in (6, 12):
        if (theday.day in (2, 3) and theday.weekday() == 0) or (
            theday.day == 1 and theday.weekday() < 5
        ):
            logging.info('Calling Filter for Come cotas operation.')     
            return True
    
    logging.info('Skpping filter for Come cotas operation.')     

    return False


def filter(file_path: str):

    import pandas

    data = pandas.read_json(file_path)

    operations_to_keep = []


def _check_for_retroactive_updates(teste):
    print(teste)


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

    push_cotista_op = SQLAlchemyOperator(
        conn_id="postgres_userdata",
        task_id="push_cotista_op",
        python_callable=_push_cotista_op,
        op_kwargs={"file_path": "/opt/airflow/data/britech/operacoes/{{ds}}.json"},
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    teste = PythonOperator(
        task_id="teste",
        python_callable=_check_for_retroactive_updates,
        op_kwargs={
            "teste": "{{macros.previous_task.get_previous_ti_sucess(task_instance=task_instance)}}"
        },
    )
    chain(
        is_business_day,
        fetch_cotista_op,
        check_for_come_cotas,
        cc_filter,
        push_cotista_op,
        teste,
    )


# TODO :
# Entre a ultima data de execucao e essa. Procura todos as operacoes tipo 4 ou 101 que tem data liquidacao / conversão dentro desse periodo.
#


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
