from airflow.sensors.base import PokeReturnValue
from airflow.sensors.base import BaseSensorOperator


class AnbimaSensor_V2(BaseSensorOperator):
    template_fields = ("_date",)

    def __init__(
        self,
        endpoint,
        response_check=None,
        date="{{ds}}",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._date = date
        self.endpoint = endpoint
        self.response_check = response_check
    
    def poke(self,context) -> PokeReturnValue:
        self.log.info("Poking: %s", self.endpoint)
        return PokeReturnValue(True,'string')
    



from airflow import DAG
from pendulum import datetime
from airflow.decorators import task
from airflow.operators.python import PythonOperator


def print_xcom(**context):
    payload = ['ti'].xcom_pull(task_ids='wait')

    print(string)



with DAG(
    dag_id="06_sensor",
    description=None,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):

    wait = AnbimaSensor_V2(
        task_id="wait",
        date="{{ds}}",
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        response_check=None,
        mode="reschedule",
        timeout=60,
        xcom_push = True,
        
    )

    teste = PythonOperator(task_id='print', python_callable=print_xcom.xcom)
    teste.xcom_pull(task_ids='wait')

