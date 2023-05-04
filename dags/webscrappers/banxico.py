import requests
from flask_api.models.currency import StageExchangeRates
from operators.custom_sql import SQLCheckOperator
from operators.write_audit_publish import InsertSQLOperator
from pendulum import datetime, parser

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.sensors.base import PokeReturnValue

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "mode": "reschedule",
    "timeout": 60 * 60,
    "conn_id": "mssql-default",
    "database": "DB_Brasil",
}

with DAG("ws_banxico", schedule=' ', default_args=default_args, catchup=False):
    #   FIXME : SELECT * FROM HOLIDAYS ???? WHERE IS TH ID....

    is_business_day = SQLCheckOperator(
        task_id="is_business_day",
        sql="SELECT CASE WHEN EXISTS (SELECT * FROM HOLIDAYS WHERE id = 1 and cast(date as date) = '{{ data_interval_start }}') then 0 else 1 end;",
        skip_on_failure=True,
    )

    @task.sensor(mode="reschedule", timeout=60 * 60)
    def scrape_banxico() -> PokeReturnValue:
        url = "https://www.banxico.org.mx/canales/singleFix.json"

        payload = ""
        headers = {
            "cookie": "TS604574e3027=083fd6a492ab200039f8ed74dad9278f84898a6fe9d5ed66124e54de667d377eadcf02425c7ab03f081cbff094113000d7a4854dfd9d8b544088c674d82894c64ede3290cd655a9ca6c25c83debe20d5cb318b0fc6e16ee189c108166a165669; TS012f422b=01ab44a5a883fdce54af0e8184a48ed4d2eab3a7e17583090df3467966dfdcb5fe0363e71b6ecaf5c820f8082b89b8a70361e5309128c170c159108dd9f0410139a53213b407c5c37eaac5c5049cb840d2e36261eeb7739bf20fce0aa33a77047a4e94f3aa",
            "Cookie": "Hex15801680=\u0021acl2tUbI4fj0uV/405nf5XFqnQVk9C1MUeQvh0PgwCDJ2B4UB8hlT47aImHjDvQ5MUxpTNmzSizOseg=; SRVCOOKIE=\u0021oNDaBJcs6TfCT7L405nf5XFqnQVk9O2+9+DZPxHSyQWBYWEqoouD8222uOR8acIAg8f0rn1Yh6KsUh0=; TS012f422b=01ab44a5a8329ced3dfc3185d064be0b73f093cf659e2e4e4ce17b3507291da73debcfd39b9da983c16ed8a07797a3e6c725d603266a6fc3c087d112be0f330b9bbec311ba7cbf091fee45af6f0aa5e2eaf3c950a2a1c5a7b8802c169e9de72b8a195542da; TS604574e3027=083fd6a492ab2000cb38b4d8e980ea2cae1e6a995b1b0c93d3e9ae2e4280a5a6aa2896a35d8d533608e76de9c01130001e83e9d1abe77da08a7d68c07541a747a2e105daf8aa58a34ed3e3151e09728ffef35505dd6f65b900bc9421a8dd0e80",
        }

        r = requests.request("GET", url, data=payload, headers=headers)

        condition_met = False
        operator_return_value = None

        if r.status_code == 200:
            operator_return_value = r.json()

            if "fecha" in operator_return_value:
                pendulum_datetime = parser.parse(
                    operator_return_value.get("fecha"), strict=False
                )

                context = get_current_context()

                assert "task" in context

                ds = context["task"].render_template(
                    "{{macros.template_tz.convert_ts(data_interval_start)}}", context
                )

                if pendulum_datetime == ds:
                    condition_met = True
        else:
            AirflowFailException("URL returned the status code %s", r.status_code)

        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    scrapped = scrape_banxico()

    push_data = InsertSQLOperator(
        task_id="push_data",
        database="DB_Brasil",
        conn_id="mssql-default",
        table=StageExchangeRates,
        values=scrapped,
    )

    chain(is_business_day, scrapped, push_data)
