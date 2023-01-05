from airflow.sensors.base import PokeReturnValue
from airflow import DAG
from pendulum import datetime

from airflow.decorators import task

with DAG(
    dag_id="05_sensor",
    description=None,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):

    @task.sensor(
        poke_interval = 320,
        timeout = 3600,
        mode='poke'
    )
    def check_av() -> PokeReturnValue:
        return PokeReturnValue(True,xcom_value='string')
    
    @task
    def print_value(string):
        print(string)   
    
    print_value(check_av())