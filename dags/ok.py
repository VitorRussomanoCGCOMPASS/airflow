from pendulum import datetime
from airflow.operators.python import PythonOperator
from operators.custom_sql import MSSQLOperator
from sensors.sql import SqlSensor
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
    "mode": "reschedule",
    "timeout": 60 * 30,
}


def _process_xcom_2(ids: list[list[str]]) -> str:
    """
    e.g. [["10,14,17,2,3,30,31,32,35,42,43,49,50,51,9"]] => '10, 14, 17, 2, 3, 30, 31, 32, 35, 42, 43, 49, 50, 51, 9'

    Parameters
    ----------
    ids : str
        str formatted list of ids

    Returns
    -------
    str

    """
    return ids[-1][-1]


def _print(id):
    print(id)


with DAG(
    "great",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/mssql/"],
):
    # TODO : CHANGE SO WE HAVE ACTIVE.
    fetch_funds = MSSQLOperator(
        task_id="fetch_funds",
        sql="declare @ids int set @ids = {{params.ids}} EXEC dbo.prueba @ids",
        parameters={"ids": 1},
        do_xcom_push=True,
    )

    printer = PythonOperator(
        python_callable=_print, task_id="printer", op_kwargs={"id": fetch_funds.output}
    )
