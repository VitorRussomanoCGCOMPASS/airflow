from operators.api import BritechOperator
from pendulum import datetime
from sensors.britech import BritechEmptySensor

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from include.utils.is_business_day import _is_business_day

from flask_api.models.cotista_op import TempCotistaOp, CotistaOp
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from operators.custom_sql import MSSQLOperator, SQLCheckOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "catchup": False,
    "max_active_runs": 1,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}

 # 2 , 31 ,3

with DAG(
    dag_id="cotista_op",
    schedule=None,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/"],
):

    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    with TaskGroup(group_id="new_cotista_op") as new_cotista_op:

        truncate_table = MSSQLOperator(
            task_id='truncate_table',
            sql = "TRUNCATE TABLE temp_cotista_op"
        )
        
        # FIXME : DS?
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
            table=TempCotistaOp,
            values=fetch_cotista_op.output,
        )

        check_data = SQLCheckOperator(
            task_id="check_data",
            sql=""" SELECT CASE WHEN EXISTS ( SELECT * WHERE 1 =1 ) THEN 1 ELSE 0 END  """,
        )

        merge_tables = MergeSQLOperator(
            task_id="merge_tables",
            source_table=TempCotistaOp,
            target_table=CotistaOp,
        )

        clean_temp_table = MSSQLOperator(
            task_id="clean_temp_table",
            sql="DELETE FROM temp_cotista_op",
        )

        chain(
            is_business_day,
            truncate_table,
            cotista_op_sensor,
            fetch_cotista_op,
            push_data,
            check_data,
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

        update_cotista_op = SQLExecuteQueryOperator(
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
