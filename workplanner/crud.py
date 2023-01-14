import datetime as dt
from typing import Optional, Iterable

import pendulum
import sqlalchemy as sa
from script_master_helper.workplanner import schemas
from script_master_helper.workplanner.enums import Statuses, Operators

from workplanner.models import Workplan

QueryT = sa.Select | sa.Update | sa.Delete


class QueryFilter:
    def __init__(self, schema: schemas.WorkplanQuery):
        self.schema = schema

    def filter_expr(self, model_field, field_schema):  # pylint: disable=R0911,R0912
        if field_schema.operator == Operators.equal:
            return model_field == field_schema.value

        if field_schema.operator == Operators.not_equal:
            return model_field != field_schema.value

        if field_schema.operator == Operators.like:
            return model_field % field_schema.value

        if field_schema.operator == Operators.not_like:
            return ~(model_field % field_schema.value)

        if field_schema.operator == Operators.ilike:
            return model_field**field_schema.value

        if field_schema.operator == Operators.not_ilike:
            return ~(model_field**field_schema.value)

        if field_schema.operator == Operators.in_:
            return model_field.in_(field_schema.value)

        if field_schema.operator == Operators.not_in:
            return model_field.not_in(field_schema.value)

        if field_schema.operator == Operators.contains:
            return model_field.contains(field_schema.value)

        if field_schema.operator == Operators.not_contains:
            return ~(model_field.contains(field_schema.value))

        if field_schema.operator == Operators.less:
            return model_field < field_schema.value

        if field_schema.operator == Operators.less_or_equal:
            return ~(model_field <= field_schema.value)

        if field_schema.operator == Operators.more:
            return model_field > field_schema.value

        if field_schema.operator == Operators.more_or_equal:
            return ~(model_field >= field_schema.value)

        raise NotImplementedError()

    def apply(self, query: QueryT) -> QueryT:
        for name in self.schema.filter.dict(exclude_unset=True):
            field_filters = getattr(self.schema.filter, name)
            if field_filters is not None:
                model_field = getattr(Workplan, name)
                for filter_ in field_filters:
                    query = query.where(self.filter_expr(model_field, filter_))

        if self.schema.order_by:
            query = query.order_by(*self.schema.order_by)

        offset = None
        if self.schema.page is not None:
            page = self.schema.page - 1 if self.schema.page > 0 else self.schema.page
            offset = page * self.schema.limit

        return query.limit(self.schema.limit).offset(offset)

    def get_query_with_filter(self, query: QueryT = None, columns=None) -> QueryT:
        if not query:
            if columns:
                query = sa.Select(*columns).select_from(Workplan)
            else:
                query = sa.Select(Workplan)

        return self.apply(query)


def delete(
    name: str = None,
    from_time: Optional[dt.datetime] = None,
    to_time: Optional[dt.datetime] = None,
    worktimes: Iterable[pendulum.DateTime] | None = None,
    filter_schema: schemas.WorkplanQuery = None,
) -> sa.Delete:
    query = sa.delete(Workplan).returning(Workplan.id)

    if filter_schema is not None:
        query = QueryFilter(filter_schema).get_query_with_filter(query)

    if name:
        query = query.filter(Workplan.name == name)
    if from_time:
        query = query.filter(Workplan.worktime_utc >= from_time)
    if to_time:
        query = query.filter(Workplan.worktime_utc <= to_time)
    if worktimes:
        query = query.filter(Workplan.worktime_utc.in_(worktimes))

    return query


def get_by_name(name: str) -> sa.Select:
    return sa.select(Workplan).where(Workplan.name == name)


def get_by_id(id_) -> sa.Select:
    return sa.select(Workplan).where(Workplan.id == id_)


def last(name: str) -> sa.Select:
    return get_by_name(name).order_by(Workplan.worktime_utc.desc())


def count_by(*dimension_fields) -> sa.Select:
    return (
        sa.select(*dimension_fields, sa.func.count().label("count"))
        .select_from(Workplan)
        .group_by(*dimension_fields)
    )


def reset(name: str, worktimes: Iterable[pendulum.DateTime]) -> sa.Update:
    return (
        sa.update(Workplan)
        .returning(Workplan)
        .filter(Workplan.name == name, Workplan.worktime_utc.in_(worktimes))
        .values(
            {
                Workplan.status.key: Statuses.default,
                Workplan.retries.key: 0,
                Workplan.info.key: None,
                Workplan.started_utc.key: None,
                Workplan.finished_utc.key: None,
                Workplan.data.key: {},
            }
        )
    )
