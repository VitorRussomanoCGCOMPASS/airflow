import numpy
from plugins.operators.new import init_calendar
from airflow.plugins_manager import AirflowPlugin


def forward(date: str, days: int, fname="ANBIMA") -> numpy.datetime64:
    """

    ‘forward’ and ‘following’ mean to take the first valid day later in time.


    Parameters
    ----------
    :param date: anchor date in ``YYYY-MM-DD`` format to add to
    :param days: number of days to add to the date, you can use negative values

    Returns
    -------
    numpy.datetime64
    """    
    calendar = init_calendar(fname)
    return numpy.busday_offset(date, days, roll="forward", busdaycal=calendar)


class AirflowTestPlugin(AirflowPlugin):
    name = "anbima_offset"
    macros = [forward]
