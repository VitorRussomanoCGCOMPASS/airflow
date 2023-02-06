from typing import Any, Union

import numpy



def _checkfile(fname):
    # try:
    #    __location__ = Path(__file__).with_name(fname)
    # except NameError:
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
