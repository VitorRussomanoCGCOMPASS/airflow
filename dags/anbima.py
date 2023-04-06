import flask_api.models.anbima as anbima
from operators.api import AnbimaOperator
from operators.custom_sql import SQLCheckOperator, MSSQLOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from pendulum import datetime
from sensors.anbima import AnbimaSensor

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from include.utils.is_business_day import _is_business_day

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "mode": "reschedule",
    "timeout": 60 * 60,
    "catchup": False,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}


with DAG(
    "anbima",
    schedule=None,
    default_args=default_args,
):
    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
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
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
            do_xcom_push=True,
        )

        store_vna = InsertSQLOperator(
            task_id="store_vna",
            table=anbima.StageVNA,
            values=fetch_vna.output,
            normalize=False
        )

        check_vna_date = SQLCheckOperator(
            task_id="check_vna_date",
            sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_anbima_vna where data_referencia != '{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
            database="DB_Brasil",
            conn_id="mssql-default",
        )

        merge_vna_tables = MergeSQLOperator(
            task_id="merge_vna_tables",
            source_table=anbima.StageVNA,
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
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
            endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
        )

        fetch_debentures = AnbimaOperator(
            task_id="fetch_debentures",
            endpoint="/feed/precos-indices/v1/debentures/mercado-secundario",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
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
                ( SELECT * from stage_debentures where data_referencia != '{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
            database="DB_Brasil",
            conn_id="mssql-default",
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
            merge_debentures_table,
        )
    with TaskGroup(group_id="cricra") as cricra:

        clean_stage_table = MSSQLOperator(
            task_id="clean_stage_table", sql="DELETE FROM stage_anbima_cricra"
        )

        wait_cricra = AnbimaSensor(
            task_id="wait_cricra",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
            endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
        )

        fetch_cricra = AnbimaOperator(
            task_id="fetch_cricra",
            endpoint="/feed/precos-indices/v1/cri-cra/mercado-secundario",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
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
                ( SELECT * from stage_anbima_vna where data_referencia != '{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
            database="DB_Brasil",
            conn_id="mssql-default",
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
            task_id="clean_stage_table", sql="DELETE FROM stage_raw_anbima_ima"
        )

        wait_ima = AnbimaSensor(
            task_id="wait_ima",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
            endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        )

        fetch_ima = AnbimaOperator(
            task_id="fetch_ima",
            endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
            request_params={
                "data": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}"
            },
            do_xcom_push=True,
        )

        store_ima = InsertSQLOperator(
            task_id="store_ima",
            table=anbima.StageRawIMA,
            values=fetch_ima.output,
        )

        check_ima_date = SQLCheckOperator(
            task_id="check_ima_date",
            sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_raw_anbima_ima where data_referencia != '{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
            database="DB_Brasil",
            conn_id="mssql-default",
        )

        split_ima = MSSQLOperator(
            task_id="split_ima",
            sql=""" 
                INSERT INTO stage_anbima_ima(indice,	data_referencia	,variacao_ult12m,	variacao_ult24m,	numero_indice,	variacao_diaria,	variacao_anual,	variacao_mensal,	peso_indice,	quantidade_titulos,	valor_mercado,	pmr,	convexidade	,duration,	yield,	redemption_yield)
                SELECT indice,	data_referencia	,variacao_ult12m,	variacao_ult24m,	numero_indice,	variacao_diaria,	variacao_anual,	variacao_mensal,	peso_indice,	quantidade_titulos,	valor_mercado,	pmr,	convexidade	,duration,	yield,	redemption_yield
                FROM stage_raw_anbima_ima
                """,
        )
        split_ima_to_components = MSSQLOperator(
            task_id="split_ima_to_components",
            sql=""" 
            INSERT INTO stage_components_anbima_ima (indice,	data_referencia,	tipo_titulo,	data_vencimento,	codigo_selic,	codigo_isin,	taxa_indicativa,	pu,	pu_juros,	quantidade_componentes,	quantidade_teorica,	valor_mercado,	peso_componente,	prazo_vencimento,	duration,	pmr,	convexidade)
            SELECT indice,	data_referencia,	tipo_titulo,	data_vencimento,	codigo_selic,	codigo_isin,	taxa_indicativa,	pu,	pu_juros,	quantidade_componentes,	quantidade_teorica,	valor_mercado,	peso_componente,	prazo_vencimento,	duration,	pmr,	convexidade
            FROM stage_raw_anbima_ima
                """,
        )

        merge_ima_tables = MergeSQLOperator(
            task_id="merge_ima_tables",
            source_table=anbima.StageIMA,
            target_table=anbima.IMA,
        )
        merge_components_ima_tables = MergeSQLOperator(
            task_id="merge_ima_components_table",
            source_table=anbima.StageComponentsIMA,
            target_table=anbima.ComponentsIMA,
        )

        chain(
            clean_stage_table,
            wait_ima,
            fetch_ima,
            store_ima,
            check_ima_date,
            [split_ima, split_ima_to_components],
            [merge_ima_tables, merge_components_ima_tables],
        )

    chain(is_business_day, [debentures, ima, cricra, vna])
