import flask_api.models.anbima as anbima
from operators.api import AnbimaOperator
from operators.custom_sql import SQLCheckOperator, MSSQLOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from pendulum import datetime
from sensors.anbima import AnbimaSensor

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "mode": "reschedule",
    "timeout": 60 * 60,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
    "do_xcom_push": False,
}

with DAG(
    "anbima", schedule="00 05 * * MON-FRI", default_args=default_args, catchup=False
):

    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    with TaskGroup(group_id="vna") as vna:

        clean_stage_table = MSSQLOperator(
            task_id="clean_stage_table", sql="DELETE FROM stage_anbima_vna"
        )

        wait_vna = AnbimaSensor(
            task_id="wait_vna",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts), -1)}}"
            },
            endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
        )

        fetch_vna = AnbimaOperator(
            task_id="fetch_vna",
            endpoint="/feed/precos-indices/v1/titulos-publicos/vna",
            request_params={
                "data": "{{macros.template_tz.convert_ts(data_interval_start)}}"
            },
            do_xcom_push=True,
        )

        store_vna = InsertSQLOperator(
            task_id="store_vna",
            table=anbima.StageVNA,
            values=fetch_vna.output,
            normalize=False,
        )

        check_vna_date = SQLCheckOperator(
            task_id="check_vna_date",
            sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_anbima_vna where data_referencia != '{{macros.template_tz.convert_ts(data_interval_start)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
        )

        merge_vna_tables = MergeSQLOperator(
            task_id="merge_vna_tables",
            source_table=anbima.StageVNAView,
            target_table=anbima.VNA,
        )

        chain(
            clean_stage_table,
            wait_vna,
            fetch_vna,
            store_vna,
            check_vna_date,
            merge_vna_tables,
        )

    with TaskGroup(group_id="debentures") as debentures:

        clean_stage_table = MSSQLOperator(
            task_id="clean_stage_table", sql="DELETE FROM stage_debentures"
        )

        wait_debentures = AnbimaSensor(
            task_id="wait_debentures",
            request_params={
                "data": "{{macros.template_tz.convert_ts(data_interval_start)}}"
            },
            endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
        )

        fetch_debentures = AnbimaOperator(
            task_id="fetch_debentures",
            endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
            request_params={
                "data": "{{macros.template_tz.convert_ts(data_interval_start)}}"
            },
            do_xcom_push=True,
        )
        store_debentures = InsertSQLOperator(
            task_id="store_debentures",
            table=anbima.StageDebentures,
            values=fetch_debentures.output,
        )

        check_debentures_date = SQLCheckOperator(
            task_id="check_debentures_date",
            sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_debentures where data_referencia != '{{macros.template_tz.convert_ts(data_interval_start)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
        )

        transform_col_percent_reune = MSSQLOperator(
            task_id="transform_col_percent_reune",
            sql=""" 
                UPDATE stage_debentures SET percent_reune = CAST(NULLIF(REPLACE(percent_reune, '%', ''), '--') AS FLOAT) / 100 
                """,
        )

        merge_debentures_table = MergeSQLOperator(
            task_id="merge_debentures_table",
            source_table=anbima.StageDebentures,
            target_table=anbima.Debentures,
        )
        chain(
            clean_stage_table,
            wait_debentures,
            fetch_debentures,
            store_debentures,
            check_debentures_date,
            transform_col_percent_reune,
            merge_debentures_table,
        )
    with TaskGroup(group_id="cricra") as cricra:

        clean_stage_table = MSSQLOperator(
            task_id="clean_stage_table", sql="DELETE FROM stage_anbima_cricra"
        )

        wait_cricra = AnbimaSensor(
            task_id="wait_cricra",
            request_params={
                "data": "{{macros.template_tz.convert_ts(data_interval_start)}}"
            },
            endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
        )

        fetch_cricra = AnbimaOperator(
            task_id="fetch_cricra",
            endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
            request_params={
                "data": "{{macros.template_tz.convert_ts(data_interval_start)}}"
            },
            do_xcom_push=True,
        )

        store_cricra = InsertSQLOperator(
            task_id="store_cricra",
            table=anbima.StageCriCra,
            values=fetch_cricra.output,
        )

        check_cricra_date = SQLCheckOperator(
            task_id="check_cricra_date",
            sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_anbima_vna where data_referencia != '{{macros.template_tz.convert_ts(data_interval_start)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
        )

        merge_cricra_tables = MergeSQLOperator(
            task_id="merge_cricra_tables",
            source_table=anbima.StageCriCra,
            target_table=anbima.CriCra,
        )

        chain(
            clean_stage_table,
            wait_cricra,
            fetch_cricra,
            store_cricra,
            check_cricra_date,
            merge_cricra_tables,
        )

    with TaskGroup(group_id="ima") as ima:

        clean_stage_table = MSSQLOperator(
            task_id="clean_stage_table", sql="DELETE FROM stage_anbima_ima"
        )

        wait_ima = AnbimaSensor(
            task_id="wait_ima",
            request_params={
                "data": "{{macros.template_tz.convert_ts(data_interval_start)}}"
            },
            endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        )

        fetch_ima = AnbimaOperator(
            task_id="fetch_ima",
            endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
            request_params={
                "data": "{{macros.template_tz.convert_ts(data_interval_start)}}"
            },
            do_xcom_push=True,
        )

        store_ima = InsertSQLOperator(
            task_id="store_ima",
            table=anbima.StageIMA,
            values=fetch_ima.output,
        )

        check_ima_date = SQLCheckOperator(
            task_id="check_ima_date",
            sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_anbima_ima where data_referencia != '{{macros.template_tz.convert_ts(data_interval_start)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
        )

        merge_ima_tables = MergeSQLOperator(
            task_id="merge_ima_tables",
            source_table=anbima.StageIMA,
            target_table=anbima.IMA,
            index_elements=(
                "indice",
                "data_referencia",
                "variacao_ult12m",
                "variacao_ult24m",
                "numero_indice",
                "variacao_diaria",
                "variacao_anual",
                "variacao_mensal",
                "peso_indice",
                "quantidade_titulos",
                "valor_mercado",
                "pmr",
                "convexidade",
                "duration",
                "yield",
                "redemption_yield",
            ),
        )
        merge_components_ima_tables = MergeSQLOperator(
            task_id="merge_ima_components_table",
            source_table=anbima.StageComponentsIMAView,
            target_table=anbima.ComponentsIMA,
        )

        chain(
            clean_stage_table,
            wait_ima,
            fetch_ima,
            store_ima,
            check_ima_date,
            merge_ima_tables,
            merge_components_ima_tables,
        )

    clean_all_stage_tables = MSSQLOperator(
        task_id="clean_all_stage_tables",
        sql=""" 
        DELETE FROM stage_anbima_vna; 
        DELETE FROM stage_anbima_ima;
        DELETE FROM stage_anbima_cricra;
        DELETE FROM stage_debentures
            """,
    )

    chain(is_business_day, [debentures, ima, cricra, vna])
