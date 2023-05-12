from pendulum import datetime
from airflow import DAG
from operators.custom_sql import MSSQLOperator, SQLCheckOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator

from operators.api import BritechOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 4, 21, tz="America/Sao_Paulo"),
}


with DAG(
    "indices_to_db",
    default_args=default_args,
    catchup=False,
    schedule="59 8-23 * * MON-FRI",
    max_active_runs=1,
):
    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
        database="DB_Brasil",
        conn_id="mssql-default",
    )

    fetch_indices = BritechOperator(
        task_id="fetch_indices",
        endpoint="/MarketData/BuscaCotacaoIndicePeriodo",
        do_xcom_push=True,
        request_params={
            "dataInicio": "{{ds}}",
            "dataFim": "{{ds}}",
            "idIndice": ";",
        },
    )
    
# TODO : INDICES STAGE TABLE
# TODO : MERGE INTO INDICES PRODUCTION TABLE.
