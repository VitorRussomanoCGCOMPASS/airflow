from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.sensors.base import PokeReturnValue
from pendulum import datetime
from plugins.operators.custom_sql import MSSQLOperator, SQLCheckOperator
from airflow.models.baseoperator import chain
from airflow.sensors.base import PokeReturnValue
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from flask_api.models.cetip import Cetip, StageCetip
from plugins.operators.write_audit_publish import MergeSQLOperator
from plugins.operators.write_audit_publish import InsertSQLOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "mode": "reschedule",
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}


def rowgetDataText(tr, coltag="td"):  # td (data) or th (header)
    return [td.get_text(strip=True) for td in tr.find_all(coltag)]


EXPECTED_HEADERS = [
    "",
    "Emissor",
    "Código do Instrumento Financeiro",
    "Preços",
    "Taxa",
    "Quantidade Negociada",
    "Número de Negócios",
    "Vol. Financeiro",
    "Preço do Último Negócio",
    "Última Taxa",
    "Origem do Último Negócio",
    "Horário de Fechamento do Último Negócio",
    "Data de liquidação do Último Negócio",
]

EXPECTED_SUBHEADERS = [
    "Máximo",
    "Médio",
    "Mínimo",
    "",
    "Média",
    "Mínima",
]


COL_HEADERS = [
    "type",
    "issuer",
    "instrument_code",
    "price_max",
    "price_mean",
    "price_min",
    "rate_max",
    "rate_mean",
    "rate_min",
    "quantity",
    "nof_trades",
    "financial_vol",
    "last_price",
    "last_rate",
    "last_trade_source",
    "last_trade_closing_time",
    "last_trade_settlement_date",
    "date",
]


with DAG(
    "cetip_ws", schedule="00 6 * * MON-FRI", default_args=default_args, catchup=False
):

    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE calendar_id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    @task.sensor(mode="reschedule", timeout=60 * 60 * 4,poke_interval = 60 * 60)
    def scrape_cetip() -> PokeReturnValue:
        import requests
        from bs4 import BeautifulSoup

        context = get_current_context() 
        ds= context.get("data_interval_start") or None
        de = context.get("data_interval_end") or None

        assert ds is not None
        assert de is not None


        url = "https://www.cetip.com.br/NegociosRegistrados"

        payload = ""
        headers = {
            "Cookie": "_gcl_au=1.1.990583558.1683038921; _ga_T6D1M7G116=GS1.1.1684255006.5.1.1684255968.60.0.0; _gid=GA1.3.1761313389.1685972074; _clck=54f1lx^|2^|fc7^|0^|1231; _ga_SS7FXRTPP3=GS1.1.1685972073.6.1.1685972085.0.0.0; _ga=GA1.1.621714615.1683038921; _clsk=ot292m^|1685972088262^|2^|1^|w.clarity.ms/collect; CetipLanguage=pt-BR; ASP.NET_SessionId=ucyzian14kj0gmbe1pvq4ke2"
        }
        r = requests.request("GET", url, data=payload, headers=headers)



        condition_met = False
        operator_return_value =  {} 

        if not r.status_code == 200:
            raise AirflowFailException("URL returned the status code %s", r.status_code)


        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.findAll("div", {"class": "conteudo-tabela"})[0]

        trs = table.findAll("tr")

        headers = rowgetDataText(trs[0], "th")
        subheaders = rowgetDataText(trs[1], "th")

        data_interval_start = ds.format('DD/MM/YY')
        data_interval_end=  de.format('DD/MM/YY')


        if headers == EXPECTED_HEADERS and subheaders == EXPECTED_SUBHEADERS:
            operator_return_value = [ {
                **dict(zip(COL_HEADERS[:-1], rowgetDataText(tr, "td"))),
                COL_HEADERS[-1]: data_interval_start
            }
            for tr in trs[2:]
        ]
 

        else:
            raise AirflowFailException("Headers or Subheaders do not match expected")

        if all(item.get("last_trade_settlement_date") in (data_interval_start, data_interval_end)  for item in operator_return_value):
                condition_met = True



        return PokeReturnValue(is_done = condition_met , xcom_value=operator_return_value)
        
    clean_landing_table= MSSQLOperator(
            task_id= 'clean_landing_table', sql= "DELETE FROM stage_cetip")
    
    scrapped = scrape_cetip()


    push_data = InsertSQLOperator(
            task_id= 'push_data',
            table=StageCetip,
            values = scrapped)
    
    make_transformations = MSSQLOperator(
            task_id='make_transformations',
            sql = """
                UPDATE stage_cetip SET 
                    price_max = dbo.FN_ToNumber(price_max) ,
                    price_mean  = dbo.FN_ToNumber(price_mean) , 
                    price_min = dbo.FN_ToNumber(price_min) ,
                    rate_max= CASE WHEN rate_max ='' THEN NULL ELSE CONVERT(float, REPLACE(rate_max, ',', '.')) END,
                    rate_mean= CASE WHEN rate_mean='' THEN NULL ELSE CONVERT(float, REPLACE(rate_mean, ',', '.'))END,
                    rate_min= CASE WHEN rate_min='' THEN NULL ELSE CONVERT(float, REPLACE(rate_min, ',', '.'))  END,
                    quantity = case when quantity='' THEN NULL ELSE CAST(REPLACE(quantity,'.', '') AS INT) END,
                    nof_trades = CASE WHEN nof_trades='' THEN NULL  ELSE CAST(REPLACE(nof_trades,'.', '') AS INT) END,
                    financial_vol  = dbo.FN_ToNumber(financial_vol) , 
                    last_price = dbo.FN_ToNumber(last_price) , 
                    last_rate= CASE WHEN last_rate='' THEN NULL ELSE CONVERT(float, REPLACE(last_rate, ',', '.'))  END,
                    last_trade_closing_time = CONVERT(time, last_trade_closing_time),
                    last_trade_settlement_date= CONVERT(date, last_trade_settlement_date, 3),
                    date= CONVERT(date, date, 3)
                    """
                        )

    check_date =  SQLCheckOperator(
            task_id= 'check_date',
            sql = "SELECT CASE WHEN EXISTS ( SELECT last_trade_settlement_date from stage_cetip where last_trade_settlement_date not in  ('{{data_interval_start.to_date_string()}}','{{data_interval_end.to_date_string()}}')) THEN 0 ELSE 1 END;")

    merge_into_prod = MergeSQLOperator(
            task_id='merge_into_prod',
            source_table = StageCetip,
            target_table = Cetip)
    
    clean_landing_table_pos = MSSQLOperator(
            task_id='clean_landing_table_pos', 
            sql=" DELETE FROM stage_cetip")

    chain(is_business_day,clean_landing_table,scrapped,push_data,make_transformations,check_date,merge_into_prod,clean_landing_table_pos)












