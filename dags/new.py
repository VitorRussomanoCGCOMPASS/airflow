from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.file_share import FileShareOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup
from operators.custom_wasb import BritechOperator, PostgresOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1, tz="America/Sao_Paulo"),
    "conn_id": "postgres",
    "database": "userdata",
    "mode": "reschedule",
    "timeout": 60 * 30,
    "max_active_runs": 1,
    "catchup": False,
}


def _merge_v2(funds_data, complementary_data, filter: bool = False):

    import pandas

    funds_data = pandas.DataFrame(funds_data)
    complementary_data = pandas.read_json(complementary_data)

    data = pandas.merge(
        funds_data, complementary_data, left_on="IdCarteira", right_on="britech_id"
    )

    if filter:
        data["diff_days"] = (
            pandas.to_datetime(data["DataReferencia"])
            - pandas.to_datetime(data["inception_date"])
        ).dt.days

        data.loc[
            data.diff_days < 190,
            [
                "RentabilidadeMes",
                "RentabilidadeDia",
                "RentabilidadeAno",
                "Rentabilidade6Meses",
                "RentabilidadeInicio",
            ],
        ] = ""


with DAG(
    dag_id="asdsa",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/include/sql/"],
    render_template_as_native_obj=True,
):
    with TaskGroup(
        group_id="external-email-subset-funds"
    ) as external_email_subset_funds:

        fetch_funds = SQLExecuteQueryOperator(
            task_id="fetch_funds",
            sql="devops_id_text.sql",
            params={
                "dag": "email_cotas_pl",
                "task_group": "external-email-subset-funds",
                "type": "funds",
            },
        )

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": fetch_funds.output,
                "dataReferencia": "{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}",
            },
        )

        @task
        def process_xcom(ids) -> tuple[int, ...]:
            from itertools import chain
            return tuple(map(int, list(chain(*ids))[-1].split(",")))



        fetch_complementary_funds_data = PostgresOperator(
            task_id="fetch_complementary_funds_data",
            conn_id="postgres",
            database="userdata",
            sql=""" 
            SELECT * FROM (WITH Worktable as (SELECT britech_id, inception_date, apelido  ,"CotaFechamento" , date , type 
            FROM funds a 
            JOIN funds_values c 
            ON a.britech_id = c.funds_id 
            WHERE britech_id = any(array{{ti.xcom_pull(task_ids='external-email-subset-funds.process_xcom')}})
            AND date = inception_date 
            OR  date ='{{macros.anbima_plugin.forward(macros.template_tz.convert_ts(ts),-2)}}'
            AND britech_id = any(array{{ti.xcom_pull(task_ids='external-email-subset-funds.process_xcom')}})
            )  
            , lagged as (SELECT *, LAG("CotaFechamento") OVER (PARTITION by apelido ORDER BY date) AS inception_cota
            FROM Worktable)
            SELECT britech_id , to_char(inception_date,'YYYY-MM-DD') inception_date , apelido, type,
            COALESCE(("CotaFechamento" - inception_cota)/inception_cota ) * 100 AS "RentabilidadeInicio"
            FROM lagged) as tb
            WHERE "RentabilidadeInicio" !=0
            """,
            results_to_dict=True,
        )

        merge_and_filter = PythonOperator(
            task_id="merge_and_filter",
            python_callable=_merge_v2,
            op_kwargs={
                "funds_data": fetch_funds_return.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": True,
            },
        )

        (
            fetch_funds
            >> fetch_funds_return
            >> process_xcom(fetch_funds.output)
            >> fetch_complementary_funds_data
            >> merge_and_filter
        )
