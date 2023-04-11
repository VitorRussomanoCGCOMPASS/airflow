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
        sql=""" SELECT CASE WHEN EXISTS ( select britech_id from funds where britech_id in ({{params.ids}})) then 1 else 0 end;""",
        hook_params={"database": "DB_Brasil"},
        do_xcom_push=False,
        parameters={"ids": 49},
        fail_parameters={"ids": 100},
    )
