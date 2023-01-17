from pendulum import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy.orm import sessionmaker

default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}


def test_import():
    pass

    # import os
    # import sys
    # sys.path.insert(0,os.path.abspath(os.path.dirname('C:/Users/Vitor Russomano/airflow/mymoduleabc')))


def add_currencies():
    from flask_api.models.indexes import Indexes

    hook = PostgresHook(postgres_conn_id="postgres_new")
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    indices = [Indexes(index="abc"), Indexes(index="bcd")]
    session.add_all(indices)
    session.commit()


def list_currencies():
    from flask_api.models.indexes import Indexes

    hook = PostgresHook(postgres_conn_id="postgres_new")
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    print(session.query(Indexes).all())


with DAG("tables", schedule="@daily", default_args=default_args, catchup=False):

    
    # teste = PythonOperator(task_id="add_currencies", python_callable=add_currencies)
    list = PythonOperator(task_id="list_currencies", python_callable=list_currencies)

    # teste >> list
