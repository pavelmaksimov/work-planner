import datetime as dt
from typing import Optional, Any, TypeVar, Literal, Union, TYPE_CHECKING, Iterator

import peewee
import pendulum
import pydantic
from pydantic import validator
from pydantic.generics import GenericModel
from pydantic.schema import Generic
from pydantic.utils import GetterDict

from work_planner.enums import Statuses, Operators

if TYPE_CHECKING:
    from work_planner import models
    from work_planner.models import WorkplanQueryT

WorkplanT = TypeVar("WorkplanT")
DataT = TypeVar("DataT")


class PeeweeGetterDict(GetterDict):
    def get(self, key: Any, default: Any = None):
        res = getattr(self._obj, key, default)
        if isinstance(res, peewee.ModelSelect):
            return list(res)
        return res


class WorkplanName(pydantic.BaseModel):
    name: pydantic.constr(max_length=255)

    def get_items(self) -> peewee.ModelSelect:
        from work_planner import crud

        return crud.get_by_name(self.name)

    def get_as_pydantic(self) -> "WorkplanListGeneric[Workplan]":
        workplans_list = [Workplan.from_orm(item) for item in self.get_items()]

        return WorkplanListGeneric[Workplan](workplans=workplans_list)


class WorkplanPK(pydantic.BaseModel):
    name: pydantic.constr(max_length=255)
    worktime_utc: pendulum.DateTime

    def get_item(self) -> "models.Workplan":
        from work_planner import crud

        return crud.get_by_pk(self.name, self.worktime_utc)

    def get_as_pydantic(self) -> Optional["Workplan"]:
        item = self.get_item()
        if item is not None:
            return Workplan.from_orm(item)


class WorkplanListGeneric(GenericModel, Generic[WorkplanT]):
    workplans: list[WorkplanT]

    def save(self) -> int:
        from work_planner import crud

        workplan_list = self.dict(exclude_unset=True)["workplans"]
        return crud.many_update(workplan_list)


class WorkplanUpdate(WorkplanPK):
    data: dict = None
    retries: int = None
    hash: str = None
    status: Literal[Statuses.LiteralT] = None

    info: Optional[str] = None
    duration: Optional[int] = None
    expires_utc: Optional[dt.datetime] = None
    started_utc: Optional[dt.datetime] = None
    finished_utc: Optional[dt.datetime] = None

    def save(self) -> Optional["Workplan"]:
        from work_planner import crud

        item = crud.update(self)
        if item is not None:
            return Workplan.from_orm(item)


class Workplan(WorkplanPK):
    data: dict
    retries: int
    hash: str
    status: Literal[Statuses.LiteralT]

    info: Optional[str]
    duration: Optional[int]
    expires_utc: Optional[dt.datetime]
    started_utc: Optional[dt.datetime]
    finished_utc: Optional[dt.datetime]
    created_utc: pendulum.DateTime
    updated_utc: pendulum.DateTime

    class Config:
        orm_mode = True
        getter_dict = PeeweeGetterDict


class FilterQueryGeneric(GenericModel, Generic[DataT]):
    """https://pydantic-docs.helpmanual.io/usage/models/#generic-models"""

    value: DataT
    operator: Operators.LiteralT = Operators.equal

    # query: Union[peewee.ModelSelect, peewee.ModelDelete, peewee.ModelUpdate],
    def filter_expr(self, model_field: peewee.Field):
        if self.operator == Operators.equal:
            return model_field == self.value

        elif self.operator == Operators.not_equal:
            return model_field == self.value

        elif self.operator == Operators.like:
            return model_field % self.value

        elif self.operator == Operators.not_like:
            return ~(model_field % self.value)

        elif self.operator == Operators.ilike:
            return model_field ** self.value

        elif self.operator == Operators.not_ilike:
            return ~(model_field ** self.value)

        elif self.operator == Operators.in_:
            return model_field.in_(self.value)

        elif self.operator == Operators.not_in:
            return model_field.not_in(self.value)

        elif self.operator == Operators.contains:
            return model_field.contains(self.value)

        elif self.operator == Operators.not_contains:
            return ~(model_field.contains(self.value))

        elif self.operator == Operators.less:
            return model_field < self.value

        elif self.operator == Operators.less_or_equal:
            return ~(model_field <= self.value)

        elif self.operator == Operators.more:
            return model_field > self.value

        elif self.operator == Operators.more_or_equal:
            return ~(model_field >= self.value)

        raise NotImplemented()

    def query_filter(self, query: "WorkplanQueryT", model_field: peewee.Field):
        return query.where(self.filter_expr(model_field))


StringT = FilterQueryGeneric[Union[list[str], str]]
IntT = FilterQueryGeneric[int]
OptionalIntT = FilterQueryGeneric[Optional[int]]
DateTimeT = FilterQueryGeneric[dt.datetime]
OptionalDateTimeT = FilterQueryGeneric[Optional[dt.datetime]]
JsonT = FilterQueryGeneric[Union[str, dict, list]]


class WorkplanFields(pydantic.BaseModel):
    field_names: list[
        Literal[
            "name",
            "worktime_utc",
            "data",
            "retries",
            "duration",
            "hash",
            "status",
            "info",
            "expires_utc",
            "started_utc",
            "finished_utc",
            "created_utc",
            "updated_utc",
        ]
    ] = None

    def iter_model_fields(self) -> Iterator[peewee.Field]:
        from work_planner import models

        for name in self.field_names:
            yield getattr(models.Workplan, name)


class WorkplanQueryFilter(WorkplanPK):
    name: list[StringT] = None
    worktime_utc: list[DateTimeT] = None
    data: list[JsonT] = None
    retries: list[IntT] = None
    duration: list[OptionalIntT] = None
    hash: list[StringT] = None
    status: list[FilterQueryGeneric[list[Literal[Statuses.LiteralT]]]] = None
    info: list[StringT] = None
    expires_utc: list[OptionalDateTimeT] = None
    started_utc: list[OptionalDateTimeT] = None
    finished_utc: list[OptionalDateTimeT] = None
    created_utc: list[DateTimeT] = None
    updated_utc: list[DateTimeT] = None

    def set_where(self, query: "WorkplanQueryT") -> "WorkplanQueryT":
        from work_planner import models

        for name in self.dict(exclude_unset=True):
            filter_field_list = getattr(self, name)
            if filter_field_list is not None:
                model_field = getattr(models.Workplan, name)
                for filter_field in filter_field_list:
                    query = filter_field.query_filter(query, model_field)

        return query

    def get_select_query(self) -> "models.Workplan.select":
        from work_planner import models

        query = models.Workplan.select()

        return self.set_where(query)

    def get_as_pydantic(self) -> WorkplanListGeneric[Workplan]:
        from work_planner import models

        query = self.get_select_query()

        return models.Workplan.items_to_pydantic(query)

    def count(self) -> int:
        return self.get_select_query().count()


class Error(pydantic.BaseModel):
    code: int
    message: str


class ResponseGeneric(GenericModel, Generic[DataT]):
    """https://pydantic-docs.helpmanual.io/usage/models/#generic-models"""

    data: Optional[DataT] = None
    error: Optional[Error] = None

    @validator("error", always=True)
    def check_consistency(cls, v, values):
        if v is not None and values["data"] is not None:
            raise ValueError("must not provide both data and error")
        if v is None and values.get("data") is None:
            raise ValueError("must provide data or error")
        return v


class Affected(pydantic.BaseModel):
    count: int


class CreateWorkplans(pydantic.BaseModel):
    name: str
    worktime: pendulum.DateTime
    start_time: pendulum.DateTime
    interval_timedelta: dt.timedelta
    keep_sequence: bool
    retries: int
    retry_delay: int
    notebook_hash: str = ""
    max_fatal_errors: int = 3
    update_stale_data: Optional[
        Union[pydantic.PositiveInt, list[pydantic.NegativeInt]]
    ] = None


class NextWorktime(pydantic.BaseModel):
    name: str
    total_seconds: float
