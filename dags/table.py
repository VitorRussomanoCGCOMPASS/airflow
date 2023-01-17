from pendulum import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy.orm import sessionmaker

default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}

# https://github.com/apache/airflow/discussions/26211




def list_currencies():
    from flask_api.models.indexes import Indexes

    hook = PostgresHook(postgres_conn_id="postgres_new")
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session.query(Indexes).all()


with DAG("tables", schedule="@daily", default_args=default_args, catchup=False):

    list = PythonOperator(task_id="list_currencies", python_callable=list_currencies)

