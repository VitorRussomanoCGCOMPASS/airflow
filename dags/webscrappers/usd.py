from collections import namedtuple

from flask_api.models.currency import ExchangeRates, StageExchangeRates
from operators.custom_sql import MSSQLOperator, SQLCheckOperator
from operators.write_audit_publish import InsertSQLOperator, MergeSQLOperator
from pendulum import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.sensors.base import PokeReturnValue


def _json_object_hook(d):
    return namedtuple("JsonResponse", d.keys())(*d.values())


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "mode": "reschedule",
    "timeout": 60 * 60,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}

with DAG(
    "ws_b3", schedule="00 6 * * MON-FRI", default_args=default_args, catchup=False
):

    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    @task.sensor(mode="reschedule", timeout=60 * 60)
    def scrape_b3() -> PokeReturnValue:
        import pendulum
        import requests

        from airflow.operators.python import get_current_context

        condition_met = False
        operator_return_value = {}
        

        context = get_current_context()
        ds = context.get("data_interval_start")

        url = "https://sistemaswebb3-derivativos.b3.com.br/financialIndicatorsProxy/FinancialIndicators/GetFinancialIndicators/eyJsYW5ndWFnZSI6InB0LWJyIn0="

        payload = ""
        headers = {
            "cookie": "dtCookie=v_4_srv_34_sn_D1A6E62E482884135AF0D0EC73D7D509_perc_100000_ol_0_mul_1_app-3A2fa0c7805985f6bf_1_rcs-3Acss_0; TS0134a800=016e3b076ff8e1f0a149f804a146ebf5093d54e10a9139d026899629488c7bbca70881ecbcab3deb698a560dc388f9140b877d2697; TS01871345=016e3b076fb322b06b4ad169ec00294200d78f8abde8f2029727aca4c46ecdf0d0960f922d8c8e621ac891e7ad35af386fe1c92035",
            "Cookie": "_gcl_au=1.1.1004199673.1655680152; OptanonAlertBoxClosed=2022-07-06T15:23:39.422Z; TS0134a800=016e3b076f3cc8456955cc7165b9235cad7449f631a365dc2bdb67890d97645b0faff3703f06e0579e8c4b703e91203e1c7da2a7e5; rxVisitor=1662644018779KQ23FRBNIA4S8K6G2LQDV6FVUUNC0E5C; _gid=GA1.3.576971226.1662644019; dtCookie=v_4_srv_33_sn_49CEE75E2AA39482DE889E85BBDABF54_perc_100000_ol_0_mul_1_app-3A2fa0c7805985f6bf_1_app-3Afd69ce40c52bd20e_0_rcs-3Acss_0; TS0171d45d=011d592ce141dd2c5bd953655ce0e97541f21451821801c17477444fe01194337123980e8481142ffdd94f9f3884fe267fdf3d35f4; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Sep+08+2022+15%3A29%3A06+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A1%2CC0001%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false&geolocation=%3B; TS01871345=016e3b076f472e4488676f31ca449943a81c1f86344e6e866b185900d148607d197bb2675b6b98b70495861e8a46686d3d45b2b73f; BIGipServerpool_sistemaswebb3-derivativos.b3.com.br_443_WAF=2973313034.64288.0000; _gat_UA-94042116-2=1; _ga=GA1.3.262263151.1655680153; _gat_gtag_UA_94042116_5=1; _clck=1heydeo|1|f4p|0; _clsk=16k5ebf|1662661748073|1|1|h.clarity.ms/collect; _ga_SS7FXRTPP3=GS1.1.1662661746.10.0.1662661761.0.0.0; _ga_T6D1M7G116=GS1.1.1662661746.5.0.1662661761.45.0.0; dtLatC=3; dtSa=-; rxvt=1662663562146|1662659618094; dtPC=33$61761827_596h1p33$61762135_126h1vRKTRKHRHPPRIQHMHAFMJSEOPAWHRAHND-0e0",
        }

        r = requests.request("GET", url, data=payload, headers=headers)
        

        if r.status_code == 200:

            obj_collection = list(map(_json_object_hook, r.json()))
            cupom_limpo = next(
                (x for x in obj_collection if x.securityIdentificationCode == 10008989),
            )

            if not cupom_limpo:
                raise AirflowFailException(
                    """Could not find DÓLAR CUPOM LIMPO by its pre-defined id: 10008989"""
                )

            try:

                date = pendulum.from_format(cupom_limpo.lastUpdate, "DD/MM/YYYY")
                assert ds
                
                print(ds)
                print(type(ds))
                print(date)
                print(type(date))

                if ds.is_same_day(date):
                    condition_met = True

                    # USD
                    # BRL

                    operator_return_value.update(
                        {
                            "value": cupom_limpo.value,
                            "date": ds.to_date_string(),
                            "domestic_id": 3,
                            "foreign_id": 4,
                        }
                    )

            # Fail gracefully
            except Exception as exc:
                raise exc

        else:
            raise AirflowFailException("URL returned the status code %s", r.status_code)

        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    b3_scrapped = scrape_b3()

    clean_landing_table = MSSQLOperator(
        task_id="clean_landing_table", sql="DELETE FROM stage_exchange_rates"
    )

    push_data = InsertSQLOperator(
        task_id="push_data",
        database="DB_Brasil",
        conn_id="mssql-default",
        table=StageExchangeRates,
        values=b3_scrapped,
    )

    transform_values = MSSQLOperator(
        task_id="transform_values",
        sql=""" 
            UPDATE stage_exchange_rates SET value = CAST(replace(value,',','.') as float) , date = cast(date as date) where domestic_id =3 and foreign_id = 4
                """,
    )

    check_date = SQLCheckOperator(
        task_id="check_date",
        sql="""
                SELECT CASE WHEN 
                    EXISTS 
                ( SELECT * from stage_exchange_rates where date != '{{macros.template_tz.convert_ts(data_interval_start)}}' and  domestic_id = 3 and foreign_id = 4 ) 
                THEN 0
                ELSE 1 
                END
                """,
    )

    merge_into_production = MergeSQLOperator(
        task_id="merge_into_production",
        source_table=StageExchangeRates,
        target_table=ExchangeRates,
    )

    clean_stage_table = MSSQLOperator(
        task_id="clean_stage_table",
        sql=""" 
            DELETE FROM stage_exchange_rates
            """,
    )
    chain(
        is_business_day,
        b3_scrapped,
        clean_landing_table,
        push_data,
        transform_values,
        check_date,
        merge_into_production,
        clean_stage_table,
    )
