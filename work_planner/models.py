import json as orjson
from functools import partial
from typing import Union

import pendulum
import playhouse.sqlite_ext

from work_planner import schemas
from work_planner.database import db
from work_planner.enums import Statuses
from work_planner.fields import DateTimeUTCField
from work_planner.utils import custom_encoder

WorkplanQueryT: Union["Workplan.update", "Workplan.delete", "Workplan.select"]


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
    created_utc = DateTimeUTCField(default=pendulum.now("UTC"))
    updated_utc = DateTimeUTCField(default=pendulum.now("UTC"))

    class Meta:
        database = db
        primary_key = playhouse.sqlite_ext.CompositeKey("name", "worktime_utc")

    def to_pydantic(self) -> schemas.Workplan:
        return schemas.Workplan.from_orm(self)

    @classmethod
    def items_to_pydantic(
        cls, items_or_query: Union[list["Workplan", ...], "Workplan.select"]
    ) -> schemas.WorkplanListGeneric[schemas.Workplan]:
        workplans = [item.to_pydantic() for item in items_or_query]
        return schemas.WorkplanListGeneric[schemas.Workplan](workplans=workplans)
