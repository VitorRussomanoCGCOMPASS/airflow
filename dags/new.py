import datetime

# from pendulum import datetime as p_datetime
import logging

import yaml
from dateutil import parser

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator

# TODO : THE HOLIDAY FILE SHOULD NOT BE IN THE DAGS DIRECTORY.
# TODO : THE IS_NOT_HOLIDAY WILL BE USED SEVERAL TIMES, IT SHOULD BE SOMEWHERE ELSE TO BE IMPORTED.

# COMPLETE :  CHECK FOR DS
# COMPLETE : THE DATE FORMAT IS IMPORTANT IN THIS SCENARIO.


def _is_not_holiday(ds) -> bool:
    """
    Check if execution date (ds) is a holiday or not

    Parameters
    ----------
    ds : str
        Execution date provided by airflow
    
    # FIXME : yesterday_ds_nodash -> Indicates yesterday. 
    
    Returns
    -------
    bool
        True

    """
    with open("dags/holidays.yml", "r") as f:

        doc = yaml.load(f, Loader=yaml.SafeLoader)
        ds = datetime.datetime.strptime(ds, "%Y-%m-%d")
        doc_as_datetime = [parser.parse(date, dayfirst=False) for date in doc["Data"]]
        logging.info(ds)
        logging.info(doc_as_datetime[-1])
        if ds in doc_as_datetime:
            return False
        return True


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
