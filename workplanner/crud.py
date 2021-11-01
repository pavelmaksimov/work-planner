import datetime as dt
from typing import Optional

import peewee
import pendulum

from workplanner import schemas, models
from workplanner.database import db
from workplanner.enums import Statuses
from workplanner.models import Workplan


def update(workplan: schemas.WorkplanUpdate) -> Optional[Workplan]:
    data = workplan.dict(exclude_unset=True)
    data[Workplan.updated_utc.name] = pendulum.now()
    item = get_by_pk(workplan.name, workplan.worktime_utc)

    if item is not None:
        for k, v in data.items():
            setattr(item, k, v)

        item.save()

        return item

    return None


def many_update(workplans: list[dict]) -> int:
    for i, dct in enumerate(workplans):
        dct[Workplan.updated_utc.name] = pendulum.now()
        workplans[i] = dct

    if workplans:
        fields = workplans[0].keys()
        with db.atomic():
            return (
                Workplan.insert_many(workplans, fields=fields)
                .on_conflict_replace()
                .execute()
            )

    return 0


def create_by_worktimes(
    name: str, worktimes: list[pendulum.DateTime], data: dict = None
) -> list[Workplan]:
    items = []
    with db.atomic():
        for wtime in worktimes:
            items.append(
                Workplan.create(
                    **{
                        **data,
                        Workplan.name.name: name,
                        Workplan.worktime_utc.name: wtime,
                    }
                )
            )
    return items


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


def get_by_name_and_worktimes(
    name: str, worktimes: list[pendulum.DateTime]
) -> peewee.ModelSelect:
    return (
        Workplan.select()
        .where(Workplan.name == name, Workplan.worktime_utc.in_(worktimes))
        .all()
    )


def first(name: str) -> Optional["Workplan"]:
    return Workplan.select().where(Workplan.name == name).first()


def last(name: str) -> Optional["Workplan"]:
    return (
        Workplan.select()
        .where(Workplan.name == name)
        .order_by(Workplan.worktime_utc.desc())
        .first()
    )


def count_by(*dimension_fields: peewee.Field) -> peewee.ModelSelect:
    query = Workplan.select(
        *dimension_fields, peewee.fn.Count().alias("count")
    ).group_by(*dimension_fields)
    return query


def delete(
    *,
    name: str = None,
    from_time_utc: Optional[dt.datetime] = None,
    to_time_utc: Optional[dt.datetime] = None,
    query_filter: schemas.WorkplanQueryFilter = None,
) -> int:
    query = Workplan.delete()

    if query_filter is not None:
        query = query_filter.set_where(query)

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
    delete(name=name, from_time_utc=worktime_utc, to_time_utc=worktime_utc)
    return Workplan.create(
        **{Workplan.name.name: name, Workplan.worktime_utc.name: worktime_utc}
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


def replay(name: str, worktimes: list[pendulum.DateTime], data: dict = None) -> int:
    return (
        Workplan.update(
            **{
                **(data or {}),
                Workplan.status.name: Statuses.add,
                Workplan.retries.name: Workplan.retries + 1,
                Workplan.info.name: None,
                Workplan.duration.name: None,
                Workplan.updated_utc.name: pendulum.now(),
            }
        )
        .where(Workplan.name == name, Workplan.worktime_utc.in_(worktimes))
        .execute()
    )
