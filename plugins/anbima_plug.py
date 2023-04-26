from typing import Union

import numpy

from airflow.plugins_manager import AirflowPlugin


def _checkfile(fname):
    __location__ = "include/calendar/" + fname

    try:
        _file = {"iter": open(__location__)}
    except FileNotFoundError:
        raise Exception(f"Invalid calendar {fname}")
    return _file


def init_calendar(
    fname: Union[str, None] = None, name: Union[str, None] = None, weekmask=None
) -> numpy.busdaycalendar:
    if fname:
        _file = _checkfile(fname)
    else:
        pass

    _holidays = []
    if not weekmask:
        weekmask = [1, 1, 1, 1, 1, 0, 0]

    with _file["iter"] as fcal:  # type: ignore
        for cal_reg in fcal:
            cal_reg = cal_reg.strip()
            _holidays.append(cal_reg)

    return numpy.busdaycalendar(weekmask=weekmask, holidays=_holidays)


#  FIXME: DATETIME IS NOT TIMEZONE AWARE. SO ON FRIDAYS LATE NIGHT, IT WILL SHOW AS NON BUSINESS DAY.


def forward(date: str, days: int, fname="ANBIMA") -> str:
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
    fwd_date = numpy.busday_offset(date, days, roll="forward", busdaycal=calendar)
    return numpy.datetime_as_string(fwd_date)


class Anbima(AirflowPlugin):
    name = "anbima_plugin"
    macros = [forward]
