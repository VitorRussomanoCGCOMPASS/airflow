import yaml
import datetime
from dateutil import parser
import logging


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
    with open("/opt/airflow/include/holidays.yml", "r") as f:

        doc = yaml.load(f, Loader=yaml.SafeLoader)
        ds = datetime.datetime.strptime(ds, "%Y-%m-%d")
        doc_as_datetime = [parser.parse(date, dayfirst=False) for date in doc["Data"]]
        logging.info(ds)
        if ds in doc_as_datetime:
            return False
        return True
