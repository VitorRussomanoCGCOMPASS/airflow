from flask_api.models.cotista_op import CotistaOp, StageCotistaOp
from operators.api import BritechOperator
from operators.custom_sql import MSSQLOperator, SQLCheckOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from pendulum import datetime
from sensors.britech import BritechEmptySensor

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "catchup": False,
    "max_active_runs": 1,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}

with DAG(
    dag_id="cotista_op",
    schedule=None,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/"],
):

    is_business_day = SQLCheckOperator(
        task_id="check_for_hol",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE cast(date as date) = '{{ds}}') then 0 else 1 end;",
        skip_on_failure=True,
    )
    with TaskGroup(group_id="new_cotista_op") as new_cotista_op:
        pre_clean_temp_table = MSSQLOperator(
            task_id="pre_clean_temp_table",
            sql="DELETE FROM temp_cotista_op",
        )

        # FIXME : DS? -> CHANGE TO TIMEZONE. TS ..
        cotista_op_sensor = BritechEmptySensor(
            task_id="sensor_cotista_op",
            endpoint="/Fundo/OperacaoCotistaAnalitico",
            request_params={
                "dataInicio": "{{ds}}",
                "dataFim": "{{ds}}",
                "idsCarteiras": ";",
            },
            mode="reschedule",
            timeout=60 * 30,
        )

        fetch_cotista_op = BritechOperator(
            task_id="fetch_cotista_op",
            endpoint="/Fundo/OperacaoCotistaAnalitico",
            request_params={
                "dataInicio": "{{ds}}",
                "dataFim": "{{ds}}",
                "idsCarteiras": ";",
            },
            do_xcom_push=True,
        )

        push_data = InsertSQLOperator(
            task_id="push_data",
            table=StageCotistaOp,
            values=fetch_cotista_op.output,
        )

        check_date = SQLCheckOperator(
            task_id="check_data",
            sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from temp_cotista_op where DataOperacao != '{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}'   ) 
                THEN 0
                ELSE 1 
                END
                """,
        )

        check_other_conditions = SQLCheckOperator(
            task_id="check_other_conditions",
            sql=""" SELECT CASE WHEN EXISTS ( SELECT * WHERE 1 =1 ) THEN 1 ELSE 0 END  """,
        )

        merge_tables = MergeSQLOperator(
            task_id="merge_tables",
            source_table=StageCotistaOp,
            target_table=CotistaOp,
        )

        clean_temp_table = MSSQLOperator(
            task_id="clean_temp_table",
            sql="DELETE FROM temp_cotista_op",
        )

        chain(
            is_business_day,
            pre_clean_temp_table,
            cotista_op_sensor,
            fetch_cotista_op,
            push_data,
            [check_date, check_other_conditions],
            merge_tables,
            clean_temp_table,
        )

    with TaskGroup(group_id="update_cotista_op") as update_cotista_op:

        latest_only = LatestOnlyOperator(task_id="latest_only")

        look_for_updates = BranchSQLOperator(
            task_id="look_for_updates",
            sql="op_check_for_update.sql",
            follow_task_ids_if_false=["latest_only_join"],
            follow_task_ids_if_true=["update_cotista_op"],
        )

        update_cotista_op = MSSQLOperator(
            task_id="update_cotista_op",
            sql="op_update.sql",
        )

        chain(
            new_cotista_op,
            latest_only,
            look_for_updates,
            update_cotista_op,
        )

    latest_only_join = LatestOnlyOperator(
        task_id="latest_only_join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    with TaskGroup(group_id="daily_flow_report") as daily_flow_report:

        check_data = SQLCheckOperator(
            task_id="check_data",
            sql="check_funds.sql",
        )

    chain([new_cotista_op, update_cotista_op], latest_only_join, daily_flow_report)
