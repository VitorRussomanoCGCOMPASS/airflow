from pendulum import datetime
from operators.custom_wasb import PostgresToWasbOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from anbima_plug import is_busday
from airflow.operators.python import ShortCircuitOperator
from operators.custom_wasb import BritechToWasbOperator
from operators.custom_wasb import AnbimaToWasbOperator
from operators.anbima import AnbimaOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
}


with DAG(
    dag_id="asdsa",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
):
    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=is_busday,
        provide_context=True,
        do_xcom_push=False,
    )
    new = AnbimaOperator(
            task_id="new",
            request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
            endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
            do_xcom_push=True,
            output_path='/opt/airflow/data/testeee.json'
        )  
    
    new2 = EmptyOperator(task_id='new_2')
    
    is_business_day >> new >> new2
    


#  as_dict=True

