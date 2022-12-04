import datetime as dt

import pendulum
from sqlalchemy import DateTime
from sqlalchemy.sql.type_api import TypeDecorator


class PendulumDateTime(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if isinstance(value, (dt.date, dt.datetime)) and value.tzinfo:
            value = value.astimezone(pendulum.UTC).replace(tzinfo=None, microsecond=0)

        if isinstance(value, (pendulum.Date, pendulum.DateTime)) and value.tzinfo:
            value = (
                value.astimezone(pendulum.UTC).replace(tzinfo=None).start_of("second")
            )
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = pendulum.instance(value, pendulum.UTC)
        return value
