import yaml
from pendulum import datetime
import logging
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator

# TODO : THE HOLIDAY FILE SHOULD NOT BE IN THE DAGS DIRECTORY.
# TODO : THE IS_NOT_HOLIDAY WILL BE USED SEVERAL TIMES, IT SHOULD BE SOMEWHERE ELSE TO BE IMPORTED.

def _is_not_holiday(ds) -> bool:
    """
    Check if execution date (ds) is a holiday or not

    Parameters
    ----------
    ds : datetime
        Execution date provided by airflow
        
    Returns
    -------
    bool
        True

    """    
    with open("dags/holidays.yml", "r") as f:
        logging.info(ds)
        doc  = yaml.load(f, Loader=yaml.SafeLoader)
        if (ds in doc['Data']):
            return False
        return True

    

with DAG(
    dag_id="new",
    start_date=datetime(2022, 12, 1),
    schedule_interval="0 0 * * 1-5",
    catchup=False,
):
    cond_true = ShortCircuitOperator(
        task_id="is_not_holiday", python_callable=_is_not_holiday 
    )
    # If true it will execute the downstream tasks
    # Otherwise, it will skip the downstream tasks

    ds_true = EmptyOperator(task_id="fetch_data")
    chain(cond_true, ds_true)
