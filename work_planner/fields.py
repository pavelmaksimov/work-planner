import datetime as dt
from typing import Optional, Union

import peewee
import pendulum


# перенести в схему установление таймзоны если нету
def normalize_datetime(value: Optional[dt.datetime]) -> Optional[pendulum.DateTime]:
    if not isinstance(value, pendulum.DateTime) and isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value.replace(tzinfo=pendulum.timezone("UTC"))
        value = pendulum.instance(value, tz=pendulum.timezone("UTC"))

    elif isinstance(value, str):
        value = pendulum.parse(value, tz=pendulum.timezone("UTC"))

    return value


def strftime_utc(value: dt.datetime) -> str:
    value = value.astimezone(pendulum.timezone("UTC"))
    value = value.replace(tzinfo=None, microsecond=0)
    return value.isoformat()


class DateTimeUTCField(peewee.DateTimeField):
    def python_value(self, value: str) -> pendulum.DateTime:
        if value is not None:
            return pendulum.parse(value, tz=pendulum.timezone("UTC"))

    def db_value(
        self, value: Optional[Union[dt.datetime, pendulum.DateTime]]
    ) -> Optional[str]:
        if value is not None:
            value = normalize_datetime(value)

            if value.tzinfo is None:
                raise ValueError(f"{value} timezone not set.")

            value = strftime_utc(value)

        return value
