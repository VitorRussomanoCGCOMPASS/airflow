from plugins.operators.api import BritechOperator
from plugins.operators.custom_sendgrid import SendGridOperator, EmailObject
from plugins.operators.custom_sql import MSSQLOperator, SQLCheckOperator
from plugins.operators.file_share import FileShareOperator
from pendulum import datetime
from plugins.sensors.britech import BritechIndicesSensor
from plugins.sensors.sql import SqlSensor
from FileObjects import HTML, JSON
from airflow.exceptions import AirflowFailException
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from sendgrid.helpers.mail import GroupId
from plugins.operators.api import CoreAPIOperator
from airflow.utils.trigger_rule import TriggerRule



email_schema= {
    "type": "object",
    "properties": {
        "Emails": {
            "type": "array",
            "items": {
                "type": "string",
                "format": "email",
            },
            "minItems": 1
        },
    },
    "required": ["Emails"] 
}

# TODO : add group id.
# TODO :  CHECK FOR FUNDS / CHECK FOR INDICES MUST CHECK IF ITS STRING OR WHATEVER.


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 6 , 7, tz="America/Sao_Paulo"),
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
    "mode": "reschedule",
    "poke_interval" : 60 * 60 , 
    "timeout": 60 * 60  * 4,
    "max_active_runs": 1,
    "catchup": False,
    "do_xcom_push": True,
    "hook_params": {"database": "DB_Brasil"},
}



def _process_xcom(ids: list[list[str]]) -> str:
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


def _render_template(
    html_template: str,
    indices_data: list[dict],
    complete_funds_data: list[dict],
    **context,
) -> HTML:
    """
    Renders the html template containing jinja formatting with the data.

    Returns
    -------
    HTMLXcom
        Custom object so that the xcom backend does the proper serialization and deserialization given the html structure.
        Else it will probably corrupt the file.

    """

    from jinja2 import BaseLoader, Environment, StrictUndefined

    templateEnv = Environment(loader=BaseLoader(), undefined=StrictUndefined)

    templateEnv.filters["splitdsformat"] = splitdsformat
    templateEnv.filters["percentformat"] = percentformat
    templateEnv.filters["currencyformat"] = currencyformat
    templateEnv.filters["valueformat"] = valueformat

    rtemplate = templateEnv.from_string(html_template)
    rendered_template = rtemplate.render(
        {"indices": indices_data, "funds": complete_funds_data}
    )

    return HTML(rendered_template)


def _merge(
    funds_data: list[dict], complementary_data: list[dict], filter: bool = False
) -> JSON:
    """
    Merges funds_data and complementary_data (funds name, inception date and return since inception) together
    and optionally filters the returns of the fund that has less than 190 days since inception in conformity of regulation.
/

    Parameters
    ----------
    funds_data : list
    complementary_data : str
    filter : bool, optional by default False

    Returns
    -------
    str
        Json formatted result/
    """
    import pandas

    data = pandas.merge(
        pandas.DataFrame(funds_data),
        pandas.DataFrame(complementary_data),
        left_on="IdCarteira",
        right_on="britech_id",
    )

    if filter:
        data["diff_days"] = (
            pandas.to_datetime(data["DataReferencia"])
            - pandas.to_datetime(data["inception_date"])
        ).dt.days

        data.loc[
            data.diff_days < 180,
            [
                "RentabilidadeMes",
                "RentabilidadeDia",
                "RentabilidadeAno",
                "Rentabilidade6Meses",
                "RentabilidadeInicio",
            ],
        ] = ""

    return JSON(data.to_json(orient="records"))


def splitdsformat(value) -> str:
    """Remove the Minutes, Seconds and miliseconds from date string.
    Eg. 2023-01-01T00:00:00 -> 2023-01-11"""
    return value.split("T")[0]


def percentformat(value) -> str:
    """Format float to str with percentage format"""

    if isinstance(value, str):
        return value
    return f"{value/100:.2%}".replace(".", ",")


def currencyformat(value) -> str:
    """Format float to str  with currency format"""

    return f"{value:09,.0f}".replace("R$-", "-R$").replace(",", ".")


def valueformat(value) -> str:
    """Format float replacing '.' with ','"""

    return f"{value:0,.8f}".replace(".", ",")


def _raise_if_empty(list_ids: list) -> bool | None:
    from itertools import chain

    ids = list(chain(*list_ids))

    if not ids:
        raise AirflowFailException("List of ids is empty")
    return True


with DAG(
    "email_cotas_pl_am",
    schedule="10 9 * * MON-FRI",
    catchup=False,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/mssql/"],
):

    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    with TaskGroup(group_id="funds") as funds:
        funds_sql_sensor = SqlSensor(
            task_id="funds_sql_sensor",
            sql=""" 
            WITH WorkTable(id) as 
            ( SELECT id FROM devops WHERE type='funds' and dag='email_cotas_pl_am') 
            SELECT case when (SELECT COUNT(*) FROM funds_values
            WHERE "IdCarteira" IN 
                (SELECT id FROM WorkTable )AND Data ='{{macros.template_tz.convert_ts(data_interval_start)}}') 
                = (SELECT COUNT(*) FROM WorkTable) then 1 else 0 end; """,
            do_xcom_push=False,
        )

        select_d1_funds = MSSQLOperator(
            task_id="select_d1_funds",
            sql=""" 
            SELECT string_agg(id,',') as id from devops
            JOIN funds_values 
            ON id = IdCarteira 
            WHERE 
            type='funds' and dag='external_cotas_pl'  and Data='{{macros.template_tz.convert_ts(data_interval_start)}}'
             """,
        )

        check_for_funds = PythonOperator(
            task_id="check_for_funds",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": select_d1_funds.output},
            do_xcom_push=False,
        )

        fetch_funds_return_d1 = BritechOperator(
            task_id="fetch_funds_return_d1",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": select_d1_funds.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )

        fetch_complementary_funds_data = MSSQLOperator(
            task_id="fetch_complementary_funds_data",
            sql=""" 
            SELECT britech_id, inception_date, apelido  , type 
            FROM funds where britech_id in (SELECT ID from devops where dag='external_cotas_pl' AND type='funds')
            """,
            results_to_dict=True,
        )
        merge_and_filter = PythonOperator(
            task_id="merge_and_filter",
            python_callable=_merge,
            op_kwargs={
                "funds_data": fetch_funds_return_d1.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": True,
            },
        )
        chain(
            funds_sql_sensor,
            select_d1_funds,
            check_for_funds,
            [fetch_funds_return_d1, fetch_complementary_funds_data],
            merge_and_filter,
        )
    with TaskGroup(group_id="indices") as indices:
        fetch_indices = MSSQLOperator(
            task_id="fetch_indices",
            sql="devops_id_str.sql",
            parameters={
                "dag": "email_cotas_pl",
                "task_group": "email_cotas_pl",
                "type": "indexes",
            },
            do_xcom_push=True,
        )

        check_for_indices = PythonOperator(
            task_id="check_for_indices",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": fetch_indices.output},
            do_xcom_push=False,
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
                "DataFim": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
            do_xcom_push=False,
        )

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )
        chain(fetch_indices, check_for_indices, indices_sensor, fetch_indices_return)

    fetch_template = FileShareOperator(
        task_id="fetch_template",
        azure_fileshare_conn_id="azure-fileshare-default",
        share_name="utils",
        directory_name="Templates",
        file_name="cotas_pl_template.html",
        do_xcom_push=True,
    )

    render_template = PythonOperator(
        task_id="render_template",
        python_callable=_render_template,
        op_kwargs={
            "indices_data": fetch_indices_return.output,
            "complete_funds_data": merge_and_filter.output,
            "html_template": fetch_template.output,
        },
        do_xcom_push=True,
    )

    send_email = SendGridOperator(
        task_id="send_email",
        subject="CG - COMPASS - COTAS - {{macros.template_tz.convert_ts(data_interval_start)}}",
        to=["vitor.ibanez@cgcompass.com", "vitorrussomano@outlook.com"],
        html_content=render_template.output,
        parameters={"reply_to": "vitor.ibanez@cgcompass.com", "asm": GroupId(18501)},
        is_multiple=True,
    )

    chain(is_business_day, [indices, funds])
    chain(is_business_day, fetch_template, render_template, send_email)


with DAG(
    "email_cotas_pl_pm",
    schedule="10 14 * * MON-FRI",
    catchup=False,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/mssql"],
):
    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    with TaskGroup(group_id="indices") as indices:
        fetch_indices = MSSQLOperator(
            task_id="fetch_indices",
            sql="devops_id_str.sql",
            parameters={
                "dag": "email_cotas_pl",
                "task_group": "email_cotas_pl",
                "type": "indexes",
            },
        )

        check_for_indices = PythonOperator(
            task_id="check_for_indices",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": fetch_indices.output},
            do_xcom_push=False,
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
                "DataFim": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
            do_xcom_push=False,
        )

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )
        chain(fetch_indices, check_for_indices, indices_sensor, fetch_indices_return)

    with TaskGroup(group_id="funds") as funds:
        percentage_of_funds_sensor = SqlSensor(
            task_id="percentage_of_funds_sensor",
            sql=""" 
                WITH WorkTable(id) as 
                ( SELECT id FROM devops WHERE type='funds' and dag='external_cotas_pl')
                SELECT case when (SELECT COUNT(*) FROM funds_values
                WHERE "IdCarteira" IN 
                    (SELECT id FROM WorkTable )AND Data ='{{macros.template_tz.convert_ts(data_interval_start)}}') 
                    >= {{params.percentage}}  * (SELECT COUNT(*) FROM WorkTable) then 1 else 0 end;
                    """,
            parameters={"percentage": 0.7},
        )

        select_d1_funds = MSSQLOperator(
            task_id="select_d1_funds",
            sql=""" 

            SELECT string_agg(id,',') as id 
            From devops
            JOIN funds_values 
            ON id = IdCarteira 
            WHERE 
            type='funds' and dag='external_cotas_pl'  and Data='{{macros.template_tz.convert_ts(data_interval_start)}}'
             """,
        )


        check_for_funds = PythonOperator(
            task_id="check_for_funds",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": select_d1_funds.output},
            do_xcom_push=False,
        )


        fetch_funds_return_d1 = BritechOperator(
            task_id="fetch_funds_return_d1",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": select_d1_funds.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )

        fetch_complementary_funds_data = MSSQLOperator(
            task_id="fetch_complementary_funds_data",
            sql=""" 
            SELECT britech_id, inception_date, apelido  , type 
            FROM funds where britech_id in (SELECT ID from devops where dag='external_cotas_pl' AND type='funds')
            """,
            results_to_dict=True,
        )

        merge_and_filter = PythonOperator(
            task_id="merge_and_filter",
            python_callable=_merge,
            op_kwargs={
                "funds_data": fetch_funds_return_d1.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": True,
            },
        )

        chain(
            percentage_of_funds_sensor,
            select_d1_funds,
            check_for_funds,
            [fetch_funds_return_d1, fetch_complementary_funds_data],
            merge_and_filter,
        )
        with TaskGroup("Trigger-evening") as trigger_evening:


            check_if_all_d1 = SQLCheckOperator(
                task_id="check_if_all_d1",
                sql="""declare @date date set @date = '{{macros.template_tz.convert_ts(data_interval_start)}}' EXEC dbo.SP_FUNDS_SENSOR @date""",
                skip_on_failure=True,
                negation=True
            )

            trigger_evening_cotas_pl = TriggerDagRunOperator(
                task_id="trigger_evening_cotas_pl",
                trigger_dag_id="email_cotas_pl_evening",
                execution_date="{{ds}}",
            )
            chain(check_if_all_d1, trigger_evening_cotas_pl)

    chain(check_for_funds, trigger_evening)

    fetch_template = FileShareOperator(
        task_id="fetch_template",
        azure_fileshare_conn_id="azure-fileshare-default",
        share_name="utils",
        directory_name="Templates",
        file_name="cotas_pl_template.html",
        do_xcom_push=True,
    )

    render_template = PythonOperator(
        task_id="render_template",
        python_callable=_render_template,
        op_kwargs={
            "indices_data": fetch_indices_return.output,
            "complete_funds_data": merge_and_filter.output,
            "html_template": fetch_template.output,
        },
        do_xcom_push=True,
    )

    fetch_contacts = CoreAPIOperator(
            task_id = "fetch_contacts",
            endpoint = "/ContactoMarca",
            request_params = {"nombreMarca":"Email_Cotas","nickname":" ","idContrato":" ","idCuenta":" "},json_schema = email_schema)

    send_email = SendGridOperator(
        task_id="send_email",
        subject="CG - COMPASS - COTAS - {{macros.template_tz.convert_ts(data_interval_start)}}",
        to = EmailObject(fetch_contacts.output),
        html_content=render_template.output,
        parameters={"reply_to": "vitor.ibanez@cgcompass.com", "asm": GroupId(18501)},
        is_multiple=True,
    )


    chain(is_business_day, [indices, funds])
    chain(is_business_day, fetch_template, render_template, fetch_contacts, send_email)


with DAG(
    "email_cotas_pl_prev",
    catchup=False,
    schedule="0 9 * * MON-FRI",
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/mssql"],
):

    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    with TaskGroup(group_id="indices") as indices:

        fetch_indices = MSSQLOperator(
            task_id="fetch_indices",
            sql="devops_id_str.sql",
            parameters={
                "dag": "email_cotas_pl",
                "task_group": "email_cotas_pl",
                "type": "indexes",
            },
        )

        check_for_indices = PythonOperator(
            task_id="check_for_indices",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": fetch_indices.output},
            do_xcom_push=False,
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
                "DataFim": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
            do_xcom_push=False,
        )

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )

        chain(fetch_indices, check_for_indices, indices_sensor, fetch_indices_return)

    with TaskGroup(group_id="funds") as funds:

        fetch_funds = MSSQLOperator(
            task_id="fetch_funds",
            sql="devops_id_str.sql",
            parameters={
                "dag": "email_cotas_pl_prev",
                "task_group": "email_cotas_pl_prev",
                "type": "funds",
            },
        )

        check_for_funds = PythonOperator(
            task_id="check_for_funds",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": fetch_funds.output},
            do_xcom_push=False,
        )

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": fetch_funds.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )

        process_xcom = PythonOperator(
            task_id="process_xcom",
            python_callable=_process_xcom,
            do_xcom_push=True,
            op_kwargs={"ids": fetch_funds.output},
        )

        fetch_complementary_funds_data = MSSQLOperator(
            task_id="fetch_complementary_funds_data",
            sql=""" 
                SELECT britech_id,  inception_date, apelido ,type 
                FROM funds WHERE britech_id in ({{params.ids}})
                """,
            results_to_dict=True,
            parameters={"ids": process_xcom.output},
        )

        merge = PythonOperator(
            task_id="merge",
            python_callable=_merge,
            op_kwargs={
                "funds_data": fetch_funds_return.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": False,
            },
            do_xcom_push=True,
        )

        chain(fetch_funds, check_for_funds, [process_xcom, fetch_funds_return])

    fetch_template = FileShareOperator(
        task_id="fetch_template",
        share_name="utils",
        directory_name="Templates",
        file_name="prev_internal_cotas_template.html",
        do_xcom_push=True,
    )

    render_template = PythonOperator(
        task_id="render_template",
        python_callable=_render_template,
        op_kwargs={
            "html_template": fetch_template.output,
            "complete_funds_data": merge.output,
            "indices_data": fetch_indices_return.output,
        },
        do_xcom_push=True,
    )

    send_email = SendGridOperator(
        task_id="send_email",
        subject="CG - COMPASS - COTAS PRÉVIAS",
        to=["vitor.ibanez@cgcompass.com", "vitorrussomano@outlook.com"],
        html_content=render_template.output,
        parameters={"reply_to": "vitor.ibanez@cgcompass.com", "asm": GroupId(18501)},
        is_multiple=True,
    )

    chain(is_business_day, [indices, funds])
    chain([merge, fetch_indices_return], render_template)
    chain(is_business_day, fetch_template, render_template, send_email)


with DAG(
    "email_cotas_pl_evening",
    schedule=None,
    catchup=False,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/mssql"],
):

    with TaskGroup(group_id="indices") as indices:
        fetch_indices = MSSQLOperator(
            task_id="fetch_indices",
            sql="devops_id_str.sql",
            parameters={
                "dag": "email_cotas_pl",
                "task_group": "email_cotas_pl",
                "type": "indexes",
            },
        )

        check_for_indices = PythonOperator(
            task_id="check_for_indices",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": fetch_indices.output},
            do_xcom_push=False,
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
                "DataFim": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
            do_xcom_push=False,
        )

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )

        chain(fetch_indices, check_for_indices, indices_sensor, fetch_indices_return)

    with TaskGroup(group_id="funds") as funds:
        funds_sql_sensor = SqlSensor(
            task_id="funds_sql_sensor",
            sql=""" 
                WITH WorkTable(id) as 
                ( SELECT id FROM devops WHERE type='funds' and dag='external_cotas_pl') 
                SELECT case when (SELECT COUNT(*) FROM funds_values
                WHERE "IdCarteira" IN 
                    (SELECT id FROM WorkTable )AND Data ='{{macros.template_tz.convert_ts(data_interval_start)}}') 
                    = (SELECT COUNT(*) FROM WorkTable) then 1 else 0 end; """,
            do_xcom_push=False,
        )

        select_funds = MSSQLOperator(
            task_id="select_funds",
            sql=""" 
            SELECT string_agg(id,',') as id from devops
            JOIN funds_values 
            ON id = IdCarteira 
            WHERE 
            type='funds' and dag='external_cotas_pl'  and Data='{{macros.template_tz.convert_ts(data_interval_start)}}' """,
            do_xcom_push=True,
        )

        check_for_funds = PythonOperator(
            task_id="check_for_funds",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": select_funds.output},
            do_xcom_push=False,
        )
    
        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": select_funds.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )

        fetch_complementary_funds_data = MSSQLOperator(
            task_id="fetch_complementary_funds_data",
            sql=""" 
                SELECT britech_id, inception_date, apelido  , type 
                FROM funds where britech_id in (SELECT ID from devops where dag='external_cotas_pl' AND type='funds')
                """,
            results_to_dict=True,
        )

        merge_and_filter = PythonOperator(
            task_id="merge_and_filter",
            python_callable=_merge,
            op_kwargs={
                "funds_data": fetch_funds_return.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": True,
            },
            do_xcom_push=True,
        )

        chain(
            funds_sql_sensor,
            select_funds,
            check_for_funds,
            [fetch_funds_return, fetch_complementary_funds_data],
            merge_and_filter,
        )

    with TaskGroup(group_id="report") as report:
        fetch_template = FileShareOperator(
            task_id="fetch_template",
            azure_fileshare_conn_id="azure-fileshare-default",
            share_name="utils",
            directory_name="Templates",
            file_name="cotas_pl_template.html",
            do_xcom_push=True,
        )

        render_template = PythonOperator(
            task_id="render_template",
            python_callable=_render_template,
            op_kwargs={
                "indices_data": fetch_indices_return.output,
                "complete_funds_data": merge_and_filter.output,
                "html_template": fetch_template.output,
            },
            do_xcom_push=True,
        )
        send_email = SendGridOperator(
        task_id="send_email",
        subject="CG - COMPASS - COTAS - {{macros.template_tz.convert_ts(data_interval_start)}}",
        to=["vitor.ibanez@cgcompass.com", "vitorrussomano@outlook.com"],
        html_content=render_template.output,
        parameters={"reply_to": "vitor.ibanez@cgcompass.com", "asm": GroupId(18501)},
        is_multiple=True,
    )

        chain(fetch_template, render_template, send_email)
    chain([funds, indices], report)





with DAG(
    "email_cotas_pl_evening_complete",
    schedule="38 16 * * MON-FRI",
    catchup=False,
    default_args=default_args,
    template_searchpath=["/opt/airflow/include/sql/mssql"],
):

    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    with TaskGroup(group_id="all-funds") as all_funds:

        active_funds_sensor = SqlSensor(
                task_id="complete_funds_sql_sensor",
                sql="""declare @date date set @date = '{{macros.template_tz.convert_ts(data_interval_start)}}' EXEC dbo.SP_FUNDS_SENSOR @date""",
                do_xcom_push=False,
            )  # type: ignore
        
        # This will be executed in case of a failure. 
        percentage_of_funds_sensor = SqlSensor(
            task_id="percentage_of_funds_sensor",
            sql=""" 
                WITH WorkTable(id) as 
                (SELECT britech_id from funds where closure_date >= '{{macros.template_tz.convert_ts(data_interval_start)}}' or closure_date is null)
                SELECT case when (SELECT COUNT(*) FROM funds_values
                WHERE "IdCarteira" IN 
                    (SELECT id FROM WorkTable )AND Data ='{{macros.template_tz.convert_ts(data_interval_start)}}') 
                    >= {{params.percentage}}  * (SELECT COUNT(*) FROM WorkTable) then 1 else 0 end;
                    """,
            parameters={"percentage": 0.7},
            trigger_rule = TriggerRule.ONE_FAILED
        )
        
        select_funds = MSSQLOperator(
            task_id="select_funds",
            sql=""" 
            SELECT string_agg(britech_id,',') as id from funds 
            JOIN funds_values 
            ON britech_id= IdCarteira 
            WHERE 
            closure_date >= '{{macros.template_tz.convert_ts(data_interval_start)}}' or closure_date is null and Data='{{macros.template_tz.convert_ts(data_interval_start)}}' """,
            do_xcom_push=True,
            trigger_rule = TriggerRule.ONE_SUCCESS)

        check_for_funds = PythonOperator(
            task_id="check_for_funds",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": select_funds.output},
            do_xcom_push=False,
        )

        fetch_funds_return = BritechOperator(
            task_id="fetch_funds_return",
            endpoint="/Fundo/BuscaRentabilidadeFundos",
            request_params={
                "idCarteiras": select_funds.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )

        process_xcom = PythonOperator(
            task_id="process_xcom",
            python_callable=_process_xcom,
            do_xcom_push=True,
            op_kwargs={"ids": select_funds.output},
        )

        fetch_complementary_funds_data = MSSQLOperator(
            task_id="fetch_complementary_funds_data",
            sql=""" 
                SELECT britech_id,  inception_date, apelido ,type 
                FROM funds WHERE britech_id in ({{params.ids}})
                """,
            results_to_dict=True,
            parameters={"ids": process_xcom.output},
        )

        merge = PythonOperator(
            task_id="merge",
            python_callable=_merge,
            op_kwargs={
                "funds_data": fetch_funds_return.output,
                "complementary_data": fetch_complementary_funds_data.output,
                "filter": False,
            },
        )

        chain(active_funds_sensor, percentage_of_funds_sensor, select_funds)

        chain(
            active_funds_sensor,
            select_funds,
            check_for_funds,
            [fetch_funds_return, fetch_complementary_funds_data],
            merge,
        )

    with TaskGroup(group_id="indices") as indices:
        fetch_indices = MSSQLOperator(
            task_id="fetch_indices",
            sql="devops_id_str.sql",
            parameters={
                "dag": "email_cotas_pl",
                "task_group": "email_cotas_pl",
                "type": "indexes",
            },
            do_xcom_push=True,
        )

        check_for_indices = PythonOperator(
            task_id="check_for_indices",
            python_callable=_raise_if_empty,
            op_kwargs={"list_ids": fetch_indices.output},
            do_xcom_push=False,
        )

        indices_sensor = BritechIndicesSensor(
            task_id="indice_sensor",
            request_params={
                "idIndice": fetch_indices.output,
                "DataInicio": "{{macros.template_tz.convert_ts(data_interval_start)}}",
                "DataFim": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
            do_xcom_push=False,
        )

        fetch_indices_return = BritechOperator(
            task_id="fetch_indices_return",
            endpoint="/Fundo/BuscaRentabilidadeIndicesMercado",
            request_params={
                "idIndices": fetch_indices.output,
                "dataReferencia": "{{macros.template_tz.convert_ts(data_interval_start)}}",
            },
        )
        chain(fetch_indices, check_for_indices, indices_sensor, fetch_indices_return)

    fetch_template = FileShareOperator(
        task_id="fetch_template",
        azure_fileshare_conn_id="azure-fileshare-default",
        share_name="utils",
        directory_name="Templates",
        file_name="internal_cotas_template.html",
    )

    render_template = PythonOperator(
        task_id="render_template",
        python_callable=_render_template,
        op_kwargs={
            "indices_data": fetch_indices_return.output,
            "complete_funds_data": merge.output,
            "html_template": fetch_template.output,
        },
        do_xcom_push=True,
    )
    send_email = SendGridOperator(
        task_id="send_email",
        subject="CG - COMPASS - {{macros.template_tz.convert_ts(data_interval_start)}} - INTERNAL REPORT",
        to=["vitor.ibanez@cgcompass.com", "vitorrussomano@outlook.com"],
        html_content=render_template.output,
        parameters={"reply_to": "vitor.ibanez@cgcompass.com"},
        is_multiple=True,
    )
    chain(is_business_day, [indices, all_funds])
    chain(is_business_day, fetch_template, render_template, send_email)
