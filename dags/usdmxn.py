from airflow.decorators import task
from pendulum import datetime
from airflow.sensors.base import PokeReturnValue
from airflow import DAG
from operators.alchemy import SQLAlchemyOperator
from sqlalchemy.orm import Session
from include.utils.is_business_day import _is_business_day
from airflow.operators.python import ShortCircuitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}


# COMPLETE : jUST IMPORT SCHEMAS FROM FLASK_API AND OVERRIDE THE RAISE


def push_usdmxn(session: Session, file_path) -> None:
    from flask_api.schemas.currency import CurrencyValuesSchema
    from marshmallow import EXCLUDE

    import json

    with open(file_path, "r") as _file:
        data = json.load(_file)

    CurrencyValuesSchema(unknown=EXCLUDE, session=session).load(
        {
            "value": float(data["valor"]),
            "date": data["fechaEn"],
            "currency": {"id": 1027, "currency": "USDMXN"},
        }
    )


with DAG("usdmxn", schedule=None, default_args=default_args, catchup=False):

    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    @task.sensor(mode="poke", timeout=3600, poke_interval=1500)
    def _extract_data(output_path: str, date) -> PokeReturnValue:
        import json
        from pendulum import parser
        import requests

        url = "https://www.banxico.org.mx/canales/singleFix.json"

        payload = ""
        headers = {
            "cookie": "TS604574e3027=083fd6a492ab200039f8ed74dad9278f84898a6fe9d5ed66124e54de667d377eadcf02425c7ab03f081cbff094113000d7a4854dfd9d8b544088c674d82894c64ede3290cd655a9ca6c25c83debe20d5cb318b0fc6e16ee189c108166a165669; TS012f422b=01ab44a5a8accaecedd9fb0ad39686ec46b4d600b5e5fe33d2e63eb1e786dffb8d9a6af296eaee992ffcfa1f98210f1732a79bec44b0e16d6cdfa2dd4777018d9d71e287e06f8ba71977e3428e697851a42f777a89e49a4b4833b4532f315c3269725ec6d3",
            "Cookie": "Hex15801680=\u0021acl2tUbI4fj0uV/405nf5XFqnQVk9C1MUeQvh0PgwCDJ2B4UB8hlT47aImHjDvQ5MUxpTNmzSizOseg=; SRVCOOKIE=\u0021oNDaBJcs6TfCT7L405nf5XFqnQVk9O2+9+DZPxHSyQWBYWEqoouD8222uOR8acIAg8f0rn1Yh6KsUh0=; TS012f422b=01ab44a5a8329ced3dfc3185d064be0b73f093cf659e2e4e4ce17b3507291da73debcfd39b9da983c16ed8a07797a3e6c725d603266a6fc3c087d112be0f330b9bbec311ba7cbf091fee45af6f0aa5e2eaf3c950a2a1c5a7b8802c169e9de72b8a195542da; TS604574e3027=083fd6a492ab2000cb38b4d8e980ea2cae1e6a995b1b0c93d3e9ae2e4280a5a6aa2896a35d8d533608e76de9c01130001e83e9d1abe77da08a7d68c07541a747a2e105daf8aa58a34ed3e3151e09728ffef35505dd6f65b900bc9421a8dd0e80",
        }

        r = requests.request("GET", url, data=payload, headers=headers)
        data = r.json()

        data["fechaEn"] = parser.parse(data["fechaEn"], strict=False).to_date_string()  # type: ignore

        # TODO :CHECK THIS CONDITION

        if data["fechaEn"] == date:
            condition_met = True
        else:
            condition_met = False

        if condition_met:
            with open(output_path, "w") as _file:
                json.dump(data, _file)

        return PokeReturnValue(is_done=condition_met)

    _push_usdmxn = SQLAlchemyOperator(
        task_id="_push_usdmxn",
        conn_id="postgres_userdata",
        python_callable=push_usdmxn,
        provide_context=True,
        op_kwargs={
            "file_path": "/opt/airflow/data/banxico_{{ds}}.json",
        },
    )

    _extract_data(
        output_path="/opt/airflow/data/banxico_{{ds}}.json",
        date="{{ds}}",
    ).set_downstream(_push_usdmxn)
