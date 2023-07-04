from airflow import DAG
from pendulum import datetime
from operators.custom_sql import SQLCheckOperator, MSSQLOperator
from operators.api import BritechOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from airflow.models.baseoperator import chain
from sensors.britech import BritechEmptySensor
from flask_api.models.rendafixa_pos import StageRendaFixaPos, RendaFixaPos


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 5, 10, tz="America/Sao_Paulo"),
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}


with DAG(
    dag_id="rendafixa_pos",
    schedule="00 13 * * MON-FRI",
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/"],
    catchup=True,
    max_active_runs = 1 
):
    is_business_day = SQLCheckOperator(
        task_id="check_for_hol",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    pre_clean_temp_table = MSSQLOperator(
        task_id="pre_clean_temp_table",
        sql="DELETE FROM stage_rendafixa_pos",
    )

    rf_positions_sensor = BritechEmptySensor(
        task_id="rf_positions_sensor",
        endpoint="/RendaFixa/BuscaListaPosicaoRendaFixa_Data",
        request_params={
            "dataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "dataFim": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "idCliente": ";",
        },
        mode="reschedule",
        timeout=60 * 30,
    )

    fetch_rf_positions = BritechOperator(
        task_id="fetch_rf_positions",
        endpoint="/RendaFixa/BuscaListaPosicaoRendaFixa_Data",
        request_params={
            "dataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "dataFim": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "idCliente": ";",
        },
        do_xcom_push=True,
    )
    push_data = InsertSQLOperator(
        task_id="push_data",
        table=StageRendaFixaPos,
        values=fetch_rf_positions.output,
    )

    string_to_date = MSSQLOperator(
        task_id="string_to_date",
        database="DB_Brasil",
        conn_id="mssql-default",
        sql=""" 
            update stage_rendafixa_pos 
                SET DataHistorico = cast(DataHistorico as date) , 
                    DataVencimento = cast(DataVencimento as date),
                    DataOperacao = cast(DataOperacao as date), 
                    DataLiquidacao = cast(DataLiquidacao as date)
            """,
    )

    check_date = SQLCheckOperator(
        task_id="check_date",
        sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_rendafixa_pos WHERE cast(DataHistorico as date) != '{{macros.template_tz.convert_ts(data_interval_start)}}') 
                THEN 0
                ELSE 1 
                END
                """,
        database="DB_Brasil",
        conn_id="mssql-default",
    )

    check_if_not_none = SQLCheckOperator(
        task_id="check_if_not_none",
        sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_rendafixa_pos) 
                THEN 1
                ELSE 0 
                END
                """,
        database="DB_Brasil",
        conn_id="mssql-default",
    )

    merge_tables = MergeSQLOperator(
        task_id="merge_tables",
        source_table=StageRendaFixaPos,
        target_table=RendaFixaPos,
        holdlock=True,
        database="DB_Brasil",
        conn_id="mssql-default")
    

    chain(
        is_business_day,
        pre_clean_temp_table,
        rf_positions_sensor,
        fetch_rf_positions,
        push_data,
        string_to_date,
        [check_date, check_if_not_none],
        merge_tables
    )
