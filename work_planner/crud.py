import datetime as dt
from typing import Optional

import peewee
import pendulum

from work_planner import schemas, models
from work_planner.database import db
from work_planner.enums import Statuses
from work_planner.models import Workplan


def update(workplan: schemas.WorkplanUpdate) -> Optional[Workplan]:
    data = workplan.dict(exclude_unset=True)
    data[Workplan.updated_utc.name] = pendulum.now()
    item = get_by_pk(workplan.name, workplan.worktime_utc)
    if item is not None:
        item.save()

        return item


def many_update(workplans: list[dict]) -> int:
    for i, w in enumerate(workplans):
        w[Workplan.updated_utc.name] = pendulum.now()
        workplans[i] = w

    if workplans:
        fields = workplans[0].keys()
        with db.atomic():
            return (
                Workplan.insert_many(workplans, fields=fields)
                .on_conflict_replace()
                .execute()
            )


def iter_items(
    limit: int = None,
    offset: int = None,
    *,
    name: str = None,
    statuses: Optional[list[Statuses.LiteralT]] = None,
    query_filter: schemas.WorkplanQueryFilter = None,
) -> peewee.ModelSelect:
    query = (
        Workplan.select()
        .where(Workplan.name == name)
        .order_by(Workplan.worktime_utc.desc())
    )
    if query_filter is not None:
        query = query_filter.set_where(query).execute()

    if name is not None:
        query = query.where(Workplan.name == name)
    if statuses is not None:
        query = query.where(Workplan.status.in_(statuses))
    if limit is not None:
        query = query.limit(limit)
    if offset is not None:
        query = query.offset(offset)

    return query


def iter_items_of_page(
    page: int = 0,
    page_size: int = 30,
    *,
    name: str = None,
    statuses: Optional[list[Statuses.LiteralT]] = None,
    query_filter: schemas.WorkplanQueryFilter = None,
) -> peewee.ModelSelect:
    query = Workplan.select().order_by(Workplan.worktime_utc.desc())

    if query_filter is not None:
        query = query_filter.set_where(query).execute()

    if name is not None:
        query = query.where(Workplan.name == name)
    if statuses is not None:
        query = query.where(Workplan.status.in_(statuses))

    for item in query.paginate(page, page_size):
        yield item


def exists(name: str) -> bool:
    return bool(Workplan.get_or_none(Workplan.name == name))


def get_by_name(name: str) -> peewee.ModelSelect:
    return Workplan.select().where(Workplan.name == name)


def get_by_pk(name: str, worktime: pendulum.DateTime) -> Optional["Workplan"]:
    if worktime.tzinfo is not None:
        worktime = worktime.astimezone(pendulum.UTC)

    return (
        Workplan.select()
        .where(Workplan.name == name, Workplan.worktime_utc == worktime)
        .first()
    )


def first(name: str) -> Optional["Workplan"]:
    return Workplan.select().where(Workplan.name == name).first()


def last_worktime(name: str) -> Optional["Workplan"]:
    return (
        Workplan.select()
        .where(Workplan.name == name)
        .order_by(Workplan.worktime_utc.desc())
        .first()
    )


def last_updated(name: str) -> Optional["Workplan"]:
    return (
        Workplan.select()
        .where(Workplan.name == name)
        .order_by(Workplan.updated_utc.desc())
        .get()
    )


def count_by(*dimension_fields: peewee.Field) -> peewee.ModelSelect:
    query = Workplan.select(
        *dimension_fields, peewee.fn.Count().alias("count")
    ).group_by(*dimension_fields)
    return query


def clear(
    *,
    name: str = None,
    from_time_utc: Optional[dt.datetime] = None,
    to_time_utc: Optional[dt.datetime] = None,
    query_filter: schemas.WorkplanQueryFilter = None,
) -> int:
    query = Workplan.delete()

    if query_filter is not None:
        query = query_filter.set_where(query).execute()

    if name:
        query = query.where(Workplan.name == name)
    if from_time_utc:
        query = query.where(Workplan.worktime_utc >= from_time_utc)
    if to_time_utc:
        query = query.where(Workplan.worktime_utc <= to_time_utc)

    return query.execute()


def recreate(
    name: str,
    worktime_utc: pendulum.DateTime,
) -> "Workplan":
    clear(name=name, from_time_utc=worktime_utc, to_time_utc=worktime_utc)
    return Workplan.create(
        **{Workplan.name.name: name, Workplan.name.worktime_utc: worktime_utc}
    )


def many_recreate(
    *,
    name: str = None,
    filter_statuses: Optional[tuple[Statuses.LiteralT]] = None,
    from_time_utc: Optional[dt.datetime] = None,
    to_time_utc: Optional[dt.datetime] = None,
    query_filter: schemas.WorkplanQueryFilter = None,
) -> list["Workplan"]:
    query = Workplan.select(Workplan.name, Workplan.worktime_utc)

    if query_filter is not None:
        query = query_filter.set_where(query).execute()

    if name:
        query = query.where(Workplan.name == name)
    if from_time_utc:
        query = query.where(Workplan.worktime_utc >= from_time_utc)
    if to_time_utc:
        query = query.where(Workplan.worktime_utc <= to_time_utc)
    if filter_statuses:
        query = query.where(Workplan.status.in_(filter_statuses))

    return [recreate(item.name, item.worktime_utc) for item in query]


def set_status(
    name,
    new_status: Statuses.LiteralT,
    *,
    from_time_utc: Optional[dt.datetime] = None,
    to_time_utc: Optional[dt.datetime] = None,
    query_filter: schemas.WorkplanQueryFilter = None,
) -> int:
    query = models.Workplan.update(**{models.Workplan.status.name: new_status}).where(
        Workplan.name == name
    )

    if query_filter is not None:
        query = query_filter.set_where(query).execute()

    if from_time_utc:
        query = query.where(Workplan.worktime_utc >= from_time_utc)
    if to_time_utc:
        query = query.where(Workplan.worktime_utc <= to_time_utc)

    return query.execute()
