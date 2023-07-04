from airflow import DAG, XComArg
from airflow.operators.python import PythonOperator
from pendulum  import datetime
from plugins.operators.api import CoreAPIOperator
from plugins.operators.custom_sendgrid import SendGridOperator, EmailObject
from plugins.operators.file_share import FileShareOperator
from sendgrid import GroupId
from airflow.plugins.custom_plug import BritechOperator

def _gen():
    return "Hello World"

def _print(args):
    print(args)


default_args = {
    "owner":"airflow",
    "start_date":datetime(2023,1,1)}



with DAG("hello_world", default_args=default_args , schedule = None):

    hello_operator = PythonOperator(task_id='hello_task',python_callable=_gen)

    print_operator = PythonOperator(task_id='print_task',python_callable=_print, op_kwargs={"args":hello_operator.output})