from datetime import timedelta
from typing import Optional

import numpy
from pendulum.date import Date
from pendulum.datetime import DateTime
from pendulum.time import Time
from pendulum.tz import timezone
from pendulum import from_format
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from plugins.operators.new import init_calendar


from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import Timetable


UTC = timezone("UTC")


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





class AnbimaWorkDayTimeTable(Timetable):
    def __init__(self, schedule_at: Time) -> None:
        self._schedule_at = schedule_at

    def infer_data_interval(self, run_after: DateTime):
        weekday = run_after.weekday()
        if weekday in (0, 6):  # Monday and Sunday -- interval is last Friday.
            days_since_friday = (run_after.weekday() - 4) % 7
            delta = timedelta(days=days_since_friday)
        else:  # Otherwise the interval is yesterday.
            delta = timedelta(days=1)
        start = DateTime.combine((run_after - delta).date(), self._schedule_at).replace(
            UTC
        )
        # TODO : END IS OFFSET BY 0 WITH FORWARD ON NUMPY BUS DAY

        return DataInterval(start=start, end=(start + timedelta(days=1)))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if (
            last_automated_data_interval is not None
        ):  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            last_start_weekday = last_start.weekday()
            if (
                0 <= last_start_weekday < 4
            ):  # Last run on Monday through Thursday -- next is tomorrow.
                delta = timedelta(days=1)
            else:  # Last run on Friday -- skip to next Monday.
                delta = timedelta(days=(7 - last_start_weekday))
            next_start = DateTime.combine(
                (last_start + delta).date(), self._schedule_at
            ).replace(UTC)
        else:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup:
                # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(
                    next_start,
                    DateTime.combine(Date.today(), self._schedule_at).replace(UTC),
                )
            elif next_start.time() != Time.min:  # FIXME
                # If earliest does not fall on midnight, skip to the next day.
                next_day = next_start.date() + timedelta(days=1)
                next_start = DateTime.combine(next_day, self._schedule_at).replace(UTC)
            next_start_weekday = next_start.weekday()
            if next_start_weekday in (
                5,
                6,
            ):  # If next start is in the weekend, go to next Monday.
                delta = timedelta(days=(7 - next_start_weekday))
                next_start = next_start + delta
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.

        formatted_next_start = next_start.to_date_string()
        end = numpy.datetime_as_string(forward(formatted_next_start, 0))
        end = from_format(end, "%Y-%m-%d")

        return DagRunInfo.interval(
            start=next_start, end=(next_start + timedelta(days=1))
        )

    def serialize(self) -> dict:
        return {"schedule_at": self._schedule_at.isoformat()}

    @classmethod
    def deserialize(cls, value) -> Timetable:
        return cls(Time.fromisoformat(value["schedule_at"]))

    @property
    def summary(self) -> str:
        return f"after each workday, at {self._schedule_at}"


class Anbima(AirflowPlugin):
    name = "anbima_plugin"
    timetables = [AnbimaWorkDayTimeTable]
    macros = [forward]
