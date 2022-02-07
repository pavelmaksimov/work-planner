from typing import Optional

import peewee
import pendulum

from utils import strftime_utc
from pbm_helper.utils import normalize_datetime


class DateTimeUTCField(peewee.DateTimeField):
    def python_value(self, value: str) -> Optional[pendulum.DateTime]:
        if value is not None:
            return pendulum.parse(value, tz=pendulum.UTC)

        return None

    def db_value(self, value: Optional[pendulum.DateTime]) -> Optional[str]:
        if value is not None:
            value = normalize_datetime(value)

            if value.tzinfo is None:
                raise ValueError(f"{value} timezone not set.")

            value = strftime_utc(value)

        return value
