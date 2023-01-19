from pendulum import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {"owner": "airflow", "start_date": datetime(2023, 1, 1)}


from sqlalchemy import inspect


def add_currencies():

    from flask_api.models import ima, vna, debentures, cricra
    from flask_api.db import metadata

    hook = PostgresHook(postgres_conn_id="postgres_userdata")
    engine = hook.get_sqlalchemy_engine()
    metadata.drop_all(bind=engine)

    inspector = inspect(engine)
    for table_name in inspector.get_table_names():
        print(table_name)

def list_currencies():
    from flask_api.models import ima, vna, debentures, cricra
    from flask_api.db import metadata

    hook = PostgresHook(postgres_conn_id="postgres_userdata")
    engine = hook.get_sqlalchemy_engine()
    metadata.create_all(bind=engine)
    
    inspector = inspect(engine)
    for table_name in inspector.get_table_names():
        print(table_name)



with DAG("tables", schedule=None, default_args=default_args, catchup=False):

    
    drop = PythonOperator(task_id="drop_all", python_callable=add_currencies)
    create = PythonOperator(task_id="create_all", python_callable=list_currencies)

    drop >> create
