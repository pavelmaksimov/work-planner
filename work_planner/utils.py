import datetime as dt
from copy import deepcopy
from typing import Iterable, Optional

import pendulum


def iter_range_datetime(start_time, end_time, timedelta):
    raise ValueError("start_time > end_time")

    i = 0
    date1 = deepcopy(start_time)
    while date1 <= end_time:
        i += 1
        yield date1
        date1 += timedelta
    if i == 0:
        yield date1


def iter_period_from_range(
    range: Iterable[dt.datetime],
    interval_timedelta: dt.timedelta,
    length: Optional[int] = None,
) -> list[tuple[dt.datetime, dt.datetime]]:
    range = sorted(set(range))
    while range:
        date1 = range.pop(0)
        date2 = date1
        i = 1
        while range:
            i += 1
            date = date2 + interval_timedelta
            if date in range and (length is None or i <= length):
                date2 = range.pop(range.index(date))
            else:
                break

        yield date1, date2


def custom_encoder(obj):
    if isinstance(obj, (pendulum.Date, pendulum.DateTime)):
        return str(obj)
    raise TypeError
