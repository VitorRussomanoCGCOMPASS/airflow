from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime

def my_custom_function():
    raise Exception
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email':"Vitor.Ibanez@cgcompass.com"
}
with DAG('email_test',
         start_date=datetime(2022, 1, 1),
         max_active_runs=1,
         schedule_interval=None,
         default_args=default_args,
         ) as dag:

    tn = PythonOperator(
        task_id=f'python_print_date_1',
        python_callable=my_custom_function)