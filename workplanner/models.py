import json as orjson
from functools import partial
from typing import Union

import peewee
import pendulum
import playhouse.sqlite_ext

from workplanner import schemas
from workplanner.database import db
from workplanner.enums import Statuses
from workplanner.fields import DateTimeUTCField
from workplanner.utils import custom_encoder

WorkplanQueryT = Union[peewee.ModelUpdate, peewee.ModelDelete, peewee.ModelSelect]


class Workplan(playhouse.sqlite_ext.Model):
    name = playhouse.sqlite_ext.CharField()
    worktime_utc = DateTimeUTCField()

    status = playhouse.sqlite_ext.CharField(default=Statuses.add, null=False)
    hash = playhouse.sqlite_ext.CharField(default="", null=False)
    retries = playhouse.sqlite_ext.IntegerField(default=0)
    info = playhouse.sqlite_ext.TextField(null=True)
    data = playhouse.sqlite_ext.JSONField(
        default={},
        json_dumps=partial(orjson.dumps, default=custom_encoder),
        json_loads=orjson.loads,
    )
    duration = playhouse.sqlite_ext.IntegerField(null=True)
    expires_utc = DateTimeUTCField(null=True)
    started_utc = DateTimeUTCField(null=True)
    finished_utc = DateTimeUTCField(null=True)
    created_utc = DateTimeUTCField(default=pendulum.now())
    updated_utc = DateTimeUTCField(default=pendulum.now())

    class Meta:
        database = db
        primary_key = playhouse.sqlite_ext.CompositeKey("name", "worktime_utc")

    def to_pydantic(self) -> schemas.Workplan:
        return schemas.Workplan.from_orm(self)

    @classmethod
    def items_to_pydantic(
        cls, items_or_query: Union[list["Workplan"], "peewee.ModelSelect"]
    ) -> schemas.WorkplanListGeneric[schemas.Workplan]:
        workplans = [item.to_pydantic() for item in items_or_query]

        return schemas.WorkplanListGeneric[schemas.Workplan](workplans=workplans)
