from airflow import DAG
from pendulum import datetime
from operators.write_audit_publish import (
    AuditSQLOperator,
    MergeSQLOperator,
    InsertSQLOperator,
)
from operators.custom_sql import MSSQLOperator

from flask_api.models.currency import ExchangeRates, StageExchangeRates


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 5, 10, tz="America/Sao_Paulo"),
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}

with DAG(
    dag_id="aseda",
    schedule="00 13 * * MON-FRI",
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/"],
    catchup=True,
    max_active_runs=1,
):

    push_data = InsertSQLOperator(
        task_id="push_data",
        database="DB_Brasil",
        conn_id="mssql-default",
        table=StageExchangeRates,
        values=[
            {"domestic_id": 1, "foreign_id": 2, "date": "2023-05-08", "value": 12},
            {"domestic_id": 2, "foreign_id": 3, "date": "2023-05-12", "value": 10},
        ],
    )

    transform_values = MSSQLOperator(
        task_id="transform_values",
        sql=""" 
            UPDATE stage_exchange_rates SET date = cast(date as date) 
                """,
    )

    merge_into_production = MergeSQLOperator(
        task_id="merge_into_production",
        source_table=StageExchangeRates,
        target_table=ExchangeRates,
        set_=("value")
    )

    audit = AuditSQLOperator(
        task_id="audit",
        sql=""" 
            SELECT *
            FROM  exchange_rates
            WHERE (CASE WHEN value > 5 THEN 1 ELSE 0 END)
            """,
        table=ExchangeRates,
    )

    push_data >> transform_values >> merge_into_production >> audit
