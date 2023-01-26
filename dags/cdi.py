from bs4 import BeautifulSoup
import re
from airflow.decorators import task
from airflow import DAG
from pendulum import datetime
from airflow.operators.python import ShortCircuitOperator
from include.utils.is_business_day import _is_business_day
from airflow.sensors.base import PokeReturnValue


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}



with DAG("cdi", schedule=None, default_args=default_args, catchup=False):

    is_business_day = ShortCircuitOperator(
        task_id="is_business_day",
        python_callable=_is_business_day,
        provide_context=True,
    )

    @task.sensor(mode="poke", timeout=3600, poke_interval=1500)
    def _extract_data(output_path: str, date) -> PokeReturnValue:
        import json
        import requests

        url = "https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-taxas-referenciais-bmf-ptBR.asp"

        payload = ""
        headers = {
            "cookie": "ASPSESSIONIDAWQBSBSB=GHAEOICBDOHDCEMLIJKPMDFM; dtCookie=v_4_srv_34_sn_EB9DD873B941C711A58C8929E2544EE9_perc_100000_ol_0_mul_1_app-3Ae44446475f923f8e_1_rcs-3Acss_0; TS01871345=016e3b076f3b957b63e174143ffddbcd85638a165e28ed4295d7e4347dc585f580c061111c24f1aa0124ac7a1c455c2c35bc5bb50d; TS01ccf8f5=011d592ce1aaf5ad715c7937a353f7e20a70bf93e6c2234631f65e879d4618387c9bc343e5e28a27ec64a73cfd44143e393e18694e; ASPSESSIONIDAUTCRASA=MAHNOPCBPLGFKMLCEFJNNDCO",
            "Cookie": "dtCookie=v_4_srv_28_sn_3BDFEA2EC3CD541375E9D0EACBD176F7_perc_100000_ol_0_mul_1_app-3Ae44446475f923f8e_1_rcs-3Acss_0; TS01871345=016e3b076f3b957b63e174143ffddbcd85638a165e28ed4295d7e4347dc585f580c061111c24f1aa0124ac7a1c455c2c35bc5bb50d; TS01ccf8f5=016e3b076f3b957b63e174143ffddbcd85638a165e28ed4295d7e4347dc585f580c061111c24f1aa0124ac7a1c455c2c35bc5bb50d; rxVisitor=167001298177138ATM7QIR5VOU9GD1STRR1MV86UOK1PP; _ga=GA1.4.1099929827.1670012981; _gid=GA1.4.349256902.1670012981; _dc_gtm_UA-43178799-13=1; ASPSESSIONIDAUTCRASA=ILGNOPCBKLJGOHFDKOBEBJHN; rxvt=1670014789772^|1670012981773; dtPC=28^-vHCTCNDFBHJNRCFPWEFFUPJOFJWWWKURH-0e0; dtLatC=1; dtSa=true^%^7CU^%^7C-1^%^7C13^%^5Ec97^%^7C-^%^7C1670013003107^%^7C212989557_426^%^7Chttps^%^3A^%^2F^%^2Fwww2.bmf.com.br^%^2Fpages^%^2Fportal^%^2Fbmfbovespa^%^2Flumis^%^2Flum-taxas-referenciais-bmf-ptBR.asp^%^7C^%^7C^%^7C^%^7C",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.62",
        }

        r = requests.request("GET", url, data=payload, headers=headers)

        data = r.json()

        html = BeautifulSoup(r.content, "html.parser")

        att_date = html.find(
            "p", {"class": "large-text-right medium-text-right small-text-left legenda"}
        )

        if att_date is not None:
            if att_date := re.search(r"\d{2}/\d{2}/\d{4}", att_date.text):
                att_date = att_date.group()
                att_date = datetime.datetime.strptime(att_date, "%d/%m/%Y")

        # TODO :CHECK THIS CONDITION

        if att_date == datetime.datetime.strptime(date, "%Y-%m-%d"):
            condition_met = True
        else:
            condition_met = False

        if condition_met:
            with open(output_path, "w") as _file:
                json.dump(data, _file)

        return PokeReturnValue(is_done=condition_met)

    is_business_day.set_downstream(
        _extract_data(
            output_path="/opt/airflow/data/cdi_{{ds}}.json",
            date="{{macros.anbima_offset.forward(ds,-1)}}",
        )
    )
