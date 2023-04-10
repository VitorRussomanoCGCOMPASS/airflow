from pendulum import datetime
from sensors.sql import SoftSQLCheckSensor
from airflow import DAG

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
    "mode": "reschedule",
    "timeout": 60 * 30,
    "max_active_runs": 1,
    "catchup": False,
    "do_xcom_push": True,
}


with DAG(
    "tgeste",
    schedule=None,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/mssql/"],
):

    soft = SoftSQLCheckSensor(
        task_id="funds_sql_sensor",
        sql=""" 
            WITH WorkTable(id) as ( SELECT britech_id FROM funds WHERE britech_id IN ({{params.ids}})) SELECT case when (SELECT COUNT(*) FROM funds_values WHERE "IdCarteira" IN (SELECT id FROM WorkTable )AND Data ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-1)}}') = (SELECT COUNT(*) FROM WorkTable) then 1 else 0 end; """,
        hook_params={"database": "DB_Brasil"},
        do_xcom_push=False,
        parameters={"ids": "1"},
        fail_parameters={"ids": 49},
    )
