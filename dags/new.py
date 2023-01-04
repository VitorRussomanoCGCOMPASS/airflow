import datetime

# from pendulum import datetime as p_datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator

from operators.hol import _is_not_holiday

# COMPLETE :  CHECK FOR DS
# COMPLETE : THE DATE FORMAT IS IMPORTANT IN THIS SCENARIO.


# COMPLETE : WILL TEST IMPORTING THE IS_NOT_HOLIDAY
# TODO : WILL LATER ON TRY TO MOVE IT TO THE INCLUDE DIRECTORY


with DAG(
    dag_id="new",
    start_date=datetime.datetime(2022, 12, 1),
    schedule_interval="0 0 * * 1-5",
    catchup=False,
):
    cond_true = ShortCircuitOperator(
        task_id="is_not_holiday", python_callable=_is_not_holiday, provide_context=True
    )
    # If true it will execute the downstream tasks
    # Otherwise, it will skip the downstream tasks

    ds_true = EmptyOperator(task_id="fetch_data")
    chain(cond_true, ds_true)