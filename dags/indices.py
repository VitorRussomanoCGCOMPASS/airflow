from flask_api.models.indexes import (IndexValues, StageIndexValues,
                                      StageIndexView)
from operators.api import BritechOperator
from operators.custom_sql import MSSQLOperator, SQLCheckOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from pendulum import datetime

from airflow import DAG
from airflow.models.baseoperator import chain


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "max_active_runs": 1,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}

# FIXME :  IS BUSINESS DAY SHOULD SEE SAME DAY, NOT HOUR.

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

    pre_clean_temp_table = MSSQLOperator(
        task_id="pre_clean_temp_table",
        sql="DELETE FROM stage_indexes_values",
    )

    fetch_indices = BritechOperator(
        task_id="fetch_indices",
        endpoint="/MarketData/BuscaCotacaoIndicePeriodo",
        do_xcom_push=True,
        request_params={
            "dataInicio": "{{data_interval_start}}",
            "dataFim": "{{data_interval_start}}",
            "idIndice": ";",
        },
    )

    push_indices = InsertSQLOperator(
        task_id="push_indices", table=StageIndexValues, values=fetch_indices.output
    )

    check_if_none = SQLCheckOperator(
        task_id="check_if_none",
        sql="SELECT CASE WHEN EXISTS ( SELECT * from stage_indexes_values ) THEN 1 ELSE 0 END;",
    )

    check_for_date = SQLCheckOperator(
        task_id="check_for_date",
        sql="SELECT CASE WHEN EXISTS (SELECT * from stage_indexes_values_view where date != '{{data_interval_start}}')",
    )

    merge_tables = MergeSQLOperator(
        task_id="merge_tables",
        source_table=StageIndexView,
        target_table=IndexValues,
        holdlock=True,
        database="DB_Brasil",
        conn_id="mssql-default",
    )

    chain(
        is_business_day,
        pre_clean_temp_table,
        fetch_indices,
        push_indices,
        check_if_none,
        check_for_date,
        merge_tables,
    )
