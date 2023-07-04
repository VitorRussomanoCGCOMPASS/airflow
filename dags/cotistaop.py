from airflow import DAG
from pendulum import datetime
from operators.custom_sql import SQLCheckOperator, MSSQLOperator
from sensors.britech import BritechEmptySensor
from operators.api import BritechOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from flask_api.models.cotista_op import StageCotistaOp, CotistaOp
from airflow.models.baseoperator import chain

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "max_active_runs": 1,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}
with DAG(
    dag_id="newcotista_op",
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
        sql="DELETE FROM stage_cotista_op",
    )

    cotista_op_sensor = BritechEmptySensor(
        task_id="sensor_cotista_op",
        endpoint="/Fundo/OperacaoCotistaAnalitico",
        request_params={
            "dataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "dataFim": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "idsCarteiras": ";",
        },
        mode="reschedule",
        timeout=60 * 30,
    )   

    fetch_cotista_op = BritechOperator(
        task_id="fetch_cotista_op",
        endpoint="/Fundo/OperacaoCotistaAnalitico",
        request_params={
            "dataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            "dataFim": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(data_interval_start),+252)}}",
            "idsCarteiras": ";",
        },
        do_xcom_push=True,
    )
    push_data = InsertSQLOperator(
        task_id="push_data",
        table=StageCotistaOp,
        values=fetch_cotista_op.output,
    )

    string_to_date = MSSQLOperator(
        task_id="string_to_date",
        database="DB_Brasil",
        conn_id="mssql-default",
        sql=""" 
            update stage_cotista_op 
                SET DataOperacao = cast(DataOperacao as date) , 
                    DataConversao = cast(DataConversao as date),
                    DataLiquidacao = cast(DataLiquidacao as date), 
                    DataAgendamento = cast(DataAgendamento as date), 
                    DataDia = cast(DataDia as date)
                     """,
    )

    check_for_op = SQLCheckOperator(
        task_id="check_for_op",
        sql="SELECT CASE WHEN EXISTS (SELECT * from cotista_op WHERE DataOperacao = '{{macros.template_tz.convert_ts(data_interval_start)}}') THEN 1 ELSE 0 END;",
    )

    merge_tables = MergeSQLOperator(
        task_id="merge_tables",
        source_table=StageCotistaOp,
        target_table=CotistaOp,
        holdlock=True,
        database="DB_Brasil",
        conn_id="mssql-default",
        set_=(
            "ValorBruto",
            "ValorLiquido",
            "DataDia",
        ),
        index_elements=(
            "IdOperacao",
            "IdCotista",
            "NomeCotista",
            "CodigoInterface",
            "IdCarteira",
            "NomeFundo",
            "DataOperacao",
            "DataConversao",
            "DataLiquidacao",
            "DataAgendamento",
            "TipoOperacao",
            "TipoResgate",
            "IdPosicaoResgatada",
            "IdFormaLiquidacao",
            "Quantidade",
            "CotaOperacao",
            "ValorBruto",
            "ValorLiquido",
            "ValorIR",
            "ValorIOF",
            "ValorCPMF",
            "ValorPerformance",
            "PrejuizoUsado",
            "RendimentoResgate",
            "VariacaoResgate",
            "Observacao",
            "DadosBancarios",
            "CpfcnpjCarteira",
            "CpfcnpjCotista",
            "Fonte",
            "IdConta",
            "IdContaCotista",
            "CotaInformada",
            "IdAgenda",
            "IdOperacaoResgatada",
            "CodigoAnbima",
            "MovimentoCarteira",
            "SituacaoOperacao",
            "CodigoCategoriaMovimentacao",
            "TipoCotistaMovimentacao",
            "IdBoletaExterna",
            "Status",
            "IdOperacaoAuxiliar",
            "IdCategoriaMovimentacao",
        ),
        index_where="DataOperacao = '{{macros.template_tz.convert_ts(data_interval_start)}}'",
    )

    chain(
        is_business_day,
        pre_clean_temp_table,
        cotista_op_sensor,
        fetch_cotista_op,
        push_data,
        string_to_date,
        check_for_op,
        merge_tables,
    )


# TODO : TALVEZ O MERGE OPERATOR TENHA O WHERE AS A DICT. DAI PDE ESCOLHER PRA MERGE, PRA INSERT. ETC...
