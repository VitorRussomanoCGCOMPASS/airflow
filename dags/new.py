from pendulum import datetime
from operators.custom_wasb import PostgresToWasbOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from anbima_plug import is_busday
from airflow.operators.python import ShortCircuitOperator
from operators.custom_wasb import BritechToWasbOperator
from operators.custom_wasb import AnbimaToWasbOperator


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
    )
    new = AnbimaToWasbOperator(
            task_id="new",
            request_params={"data": "{{ macros.ds_add(ds, -1) }}"},
            endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
            blob_name="teste_anbima",
            container_name="rgbrprdblob"

        )

    is_business_day >> new


#  as_dict=True

