import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def testing_imports():
    import operators
    import sensors

    return None


with DAG(
    dag_id="test_customutils",
    start_date=datetime.datetime(2022, 12, 1),
    schedule_interval="0 0 * * 1-5",
    catchup=False,
) as dag:

    test_import_customutils = PythonOperator(
        task_id="test_import_customutils", python_callable=testing_imports, dag=dag
    )

    just_empty = EmptyOperator(task_id="just_empty", dag=dag)

    just_empty.set_upstream(test_import_customutils)
