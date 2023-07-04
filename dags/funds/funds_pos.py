from airflow import DAG
from pendulum import datetime
from operators.custom_sql import SQLCheckOperator, MSSQLOperator
from sensors.britech import BritechEmptySensor
from operators.api import BritechOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from flask_api.models.funds import StageFundsPos, FundsPos
from airflow.models.baseoperator import chain

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "max_active_runs": 1,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}

with DAG(
    dag_id="funds_pos",
    schedule="00 13 * * MON-FRI",
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/"],
    catchup=False,
):

    is_business_day = SQLCheckOperator(
        task_id="check_for_hol",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    pre_clean_temp_table = MSSQLOperator(
        task_id="pre_clean_temp_table",
        sql="DELETE FROM stage_funds_pos",
    )

    funds_pos_sensor = BritechEmptySensor(
        task_id="funds_pos_sensor",
        endpoint="/Fundo/BuscaPosicaoFundoPorIdCarteiraData",
        request_params={
            "dataPosicao": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "IdCarteira": ";",
        },
        mode="reschedule",
        timeout=60 * 30,
    )

    fetch_funds_pos = BritechOperator(
        task_id="fetch_funds_pos",
        endpoint="/Fundo/BuscaPosicaoFundoPorIdCarteiraData",
        request_params={
            "dataPosicao": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "IdCarteira": ";",
        },
        do_xcom_push=True,
    )
    push_data = InsertSQLOperator(
        task_id="push_data",
        table=StageFundsPos,
        values=fetch_funds_pos.output,
    )

    string_to_date = MSSQLOperator(
        task_id="string_to_date",
        database="DB_Brasil",
        conn_id="mssql-default",
        sql=""" 
                UPDATE stage_funds_pos 
                SET DataHistorico = cast(DataHistorico as date) , 
                    DataAplicacao = cast(DataAplicacao as date),
                    DataConversao = cast(DataConversao as date), 
                    DataUltimaCobrancaIR = cast(DataUltimaCobrancaIR as date)
                     """,
    )

    check_for_pos = SQLCheckOperator(
        task_id="check_for_pos",
        sql="SELECT CASE WHEN EXISTS (SELECT * from stage_funds_pos WHERE DataHistorico = '{{macros.template_tz.convert_ts(data_interval_start)}}') THEN 1 ELSE 0 END;",
        skip_on_failure=False,
    )

    merge_tables = MergeSQLOperator(
        task_id="merge_tables",
        source_table=StageFundsPos,
        target_table=FundsPos,
        holdlock=True,
        database="DB_Brasil",
        conn_id="mssql-default",
        set_=("Quantidade",),
    )

    chain(
        is_business_day,
        pre_clean_temp_table,
        funds_pos_sensor,
        fetch_funds_pos,
        push_data,
        string_to_date,
        check_for_pos,
        merge_tables,
    )
