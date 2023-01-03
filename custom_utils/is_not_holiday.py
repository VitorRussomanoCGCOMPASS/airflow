import  yaml
import logging 

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
    with open("custom_utils/holidays.yml", "r") as f:
        doc  = yaml.load(f, Loader=yaml.SafeLoader)
        logging.info(ds)
        
        if (ds in doc['Data']):
            return False
        return True
