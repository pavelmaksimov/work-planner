import datetime as dt
from typing import Optional, Union, Iterator, Sequence
from uuid import UUID

import pendulum
import pydantic
import sqlalchemy as sa
from script_master_helper.workplanner import schemas
from script_master_helper.workplanner.enums import Statuses, Error
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from workplanner import crud, filters, logger
from workplanner.models import Workplan
from workplanner.utils import (
    iter_range_datetime,
    iter_period_from_range,
    scroll_to_last_interval_time,
)


def is_create_next(
    db: Session,
    name: str,
    interval_timedelta: dt.timedelta,
) -> bool:
    last_executed_item = db.scalar(crud.last(name))
    if last_executed_item:
        result = pendulum.now() - last_executed_item.worktime_utc >= interval_timedelta
        return result
    else:
        result = False

    logger.info("Is create next [{}] {}", name, result)

    return result


def next_worktime(
    db: Session, name: str, interval_timedelta: dt.timedelta
) -> pendulum.DateTime | None:
    last_executed_item = db.scalar(crud.last(name))

    if last_executed_item:
        return scroll_to_last_interval_time(
            last_executed_item.worktime_utc, interval_timedelta
        )

    return None


def create_next_or_none(
    db: Session, name: str, interval_timedelta: dt.timedelta, *, data: dict = None
) -> Workplan | None:
    if is_create_next(db, name, interval_timedelta):
        next_wt = next_worktime(db, name, interval_timedelta)
        try:
            with db.begin_nested():
                item = Workplan(
                    **(data or {}),
                    **{
                        Workplan.name.key: name,
                        Workplan.worktime_utc.key: next_wt,
                    },
                )
                db.add(item)
        except IntegrityError:
            return None
        else:
            logger.info("Created next workplan [{}] {} {}", name, next_wt, item.id)
            return item


def fill_missing(
    db: Session,
    name: str,
    interval_timedelta: dt.timedelta,
    start_time: pendulum.DateTime,
    end_time: pendulum.DateTime = None,
    *,
    data: dict = None,
) -> list["Workplan"]:
    items = []
    end_time = end_time or pendulum.now()
    exists_worktimes = set(
        db.scalars(sa.select(Workplan.worktime_utc).filter(Workplan.name == name))
    )

    with db.begin_nested():
        for wt in iter_range_datetime(start_time, end_time, interval_timedelta):
            if wt not in exists_worktimes:
                item = Workplan(
                    **(data or {}),
                    **{
                        Workplan.name.key: name,
                        Workplan.worktime_utc.key: wt,
                    },
                )
                logger.info("Created missing workplans [{}] {}", name, wt)
                items.append(item)
                db.add(item)

    return items


def recreate_prev(
    db: Session,
    name: str,
    offset_periods: Union[pydantic.PositiveInt, list[pydantic.NegativeInt]],
    interval_timedelta: dt.timedelta,
    from_worktime: pendulum.DateTime = None,
    *,
    data: dict = None,
) -> list["Workplan"] | None:
    if isinstance(offset_periods, int):
        if offset_periods > 0:
            offset_periods = [-i for i in range(offset_periods)]
        else:
            raise ValueError("Only positive Int")
    else:
        assert all(i < 0 for i in offset_periods)

        offset_periods = [i + 1 for i in offset_periods]

    first_item = db.scalar(crud.get_by_name(name))

    if first_item:
        last_wt = from_worktime or scroll_to_last_interval_time(
            first_item.worktime_utc, interval_timedelta
        )

        worktime_list = [
            last_wt + (interval_timedelta * delta) for delta in offset_periods
        ]
        worktime_list = list(
            filter(lambda dt_: dt_ >= first_item.worktime_utc, worktime_list)
        )

        db.execute(crud.delete(name, worktimes=worktime_list))

        items = []
        for date1, date2 in iter_period_from_range(worktime_list, interval_timedelta):
            new_items = fill_missing(
                db,
                name,
                interval_timedelta=interval_timedelta,
                start_time=date1,
                end_time=date2,
                data=data,
            )
            items.extend(new_items)

        if worktime_list:
            logger.info("Recreated workplans [{}] {}", name, worktime_list)

        return items


def is_allowed_execute(
    db: Session, name: str, notebook_hash: str, *, max_fatal_errors: int
) -> bool:
    item = db.scalar(crud.last(name))

    if item and item.hash == notebook_hash:
        # Check limit fatal errors.
        query = (
            select(sa.func.count())
            .filter(Workplan.name == name)
            .filter(Workplan.hash == notebook_hash)
            .filter(Workplan.status == Statuses.fatal_error)
        )
        is_allowed = db.scalar(query) < max_fatal_errors
        if not is_allowed:
            logger.info("Many fatal errors [{}] {}", name, item.id)

        return is_allowed

    return True


def update_errors(
    db: Session, name: str, max_retries: int, retry_delay: int
) -> list[Workplan]:
    # A function that checks to see if retry_delay passes to restart.
    # next_start_time_timestamp = Workplan.finished_utc + dt.timedelta(retry_delay)
    worktimes_query = sa.select(Workplan).filter(
        Workplan.name == name,
        Workplan.status.in_(Statuses.error_statuses),
        Workplan.retries < max_retries,
        filters.not_expired,
        # (
        #     (pendulum.now() >= next_start_time_timestamp)
        #     | (Workplan.finished_utc.is_null())
        # ),
    )
    affected_workplans = []

    with db.begin_nested():
        for wp in db.scalars(worktimes_query):
            if not wp.finished_utc or pendulum.now("UTC") >= (
                wp.finished_utc + dt.timedelta(seconds=retry_delay)
            ):
                logger.info("Updated error workplans [{}] {}", name, wp.worktime_utc)
                wp.retries += 1
                affected_workplans.append(wp)

    return affected_workplans


def iter_generate_child_workplans(
    db: Session,
    name: str,
    parent_name: str,
    status_trigger: Statuses.LiteralT,
    *,
    from_worktime: pendulum.DateTime = None,
    data: Optional[dict] = None,
) -> Iterator[Workplan]:
    if status_trigger not in Statuses.all_statuses:
        raise ValueError(f"Invalid {status_trigger=}")

    subquery = sa.select(Workplan).filter(Workplan.name == name).subquery()

    parent_workplans_query = (
        sa.select(Workplan.worktime_utc)
        .filter(Workplan.name == parent_name, Workplan.status == status_trigger)
        .outerjoin(subquery, subquery.c.worktime_utc == Workplan.worktime_utc)
        .filter(subquery.c.worktime_utc == None)
    )

    print(f"{parent_workplans_query=}")

    if from_worktime:
        parent_workplans_query = parent_workplans_query.filter(
            Workplan.worktime_utc >= from_worktime
        )

    with db.begin_nested():
        for worktime_utc in db.scalars(parent_workplans_query):
            item = Workplan(
                **(data or {}),
                **{
                    Workplan.name.key: name,
                    Workplan.worktime_utc.key: worktime_utc,
                },
            )
            logger.info("Created missing workplans [{}] {}", name, worktime_utc)

            db.add(item)

            yield item


def generate_workplans(  # pylint: disable=R0913
    db: Session,
    name: str,
    start_time: pendulum.DateTime,
    interval_in_seconds: Union[int, float],
    keep_sequence: bool,
    max_retries: int,
    retry_delay: int,
    notebook_hash: str,
    max_fatal_errors: int,
    back_restarts: Optional[
        Union[pydantic.PositiveInt, list[pydantic.NegativeInt]]
    ] = None,
    extra: Optional[dict] = None,
    parent_name: str = None,
    status_trigger: str = None,
) -> Iterator[Workplan]:
    interval_timedelta = dt.timedelta(seconds=interval_in_seconds)

    with db.begin_nested():
        if parent_name:
            yield from iter_generate_child_workplans(
                db, name, parent_name, status_trigger, data=extra
            )

        elif is_allowed_execute(
            db, name, notebook_hash=notebook_hash, max_fatal_errors=max_fatal_errors
        ):
            if keep_sequence:
                fill_missing(db, name, interval_timedelta, start_time, data=extra)
            else:
                exists = db.execute(crud.get_by_name(name)).first()
                if not exists:
                    wp = Workplan(
                        **(extra or {}),
                        **{
                            Workplan.name.key: name,
                            Workplan.worktime_utc.key: scroll_to_last_interval_time(
                                start_time, interval_timedelta
                            ),
                        },
                    )
                    db.add(wp)
                    logger.info("Created first workplan [{}] {}", name, start_time)
                else:
                    next_item = create_next_or_none(
                        db, name, interval_timedelta, data=extra
                    )
                    if next_item:
                        if back_restarts:
                            # When creating the next item,
                            # elements are created to update the data for the past dates.
                            recreate_prev(
                                db,
                                name,
                                interval_timedelta=interval_timedelta,
                                offset_periods=back_restarts,
                                data=extra,
                            )

            list(update_errors(db, name, max_retries, retry_delay))
            check_expiration(db)

            yield from execute_list(db, name)


def clear_statuses_of_lost_items(db: Session) -> Sequence[Workplan]:
    return db.scalars(
        sa.update(Workplan)
        .returning(Workplan)
        .filter(Workplan.status.in_(Statuses.run_statuses))
        .values(**{Workplan.status.name: Statuses.default})
    ).all()


def execute_list(db: Session, name: str) -> Iterator[Workplan]:
    return db.scalars(
        sa.select(Workplan)
        .filter(Workplan.name == name, *filters.for_executed)
        .order_by(Workplan.worktime_utc.desc())
    )


def check_expiration(db: Session) -> Iterator[Workplan]:
    return db.scalars(
        sa.update(Workplan)
        .returning(Workplan)
        .values(
            **{Workplan.status.name: Statuses.error, Workplan.info.name: Error.expired}
        )
        .filter(filters.expired)
    )


def update(
    db: Session, schema: schemas.WorkplanUpdate, updated=None
) -> Workplan | None:
    data = schema.dict(exclude_unset=True)
    data[Workplan.updated_utc.key] = updated or pendulum.now()

    query = sa.update(Workplan).values(data).returning(Workplan)

    if schema.id:
        query = query.filter(Workplan.id == schema.id)
    else:
        query = query.filter(
            Workplan.name == schema.name, Workplan.worktime_utc == schema.worktime_utc
        )

    with db.begin_nested():
        return db.scalar(query)


def many_update(
    db: Session, schema_list: list[schemas.WorkplanUpdate]
) -> list[Workplan]:
    updated = pendulum.now()

    with db.begin_nested():
        return [update(db, schema, updated) for schema in schema_list]


def create_by_worktimes(
    db: Session, name: str, worktimes: list[pendulum.DateTime], data: dict = None
) -> list[Workplan]:
    items = []
    for wtime in worktimes:
        items.append(
            Workplan(
                **data,
                **{
                    Workplan.name.key: name,
                    Workplan.worktime_utc.key: wtime,
                },
            )
        )
    with db.begin_nested():
        db.add_all(items)

    return items


def run(db: Session, id: UUID) -> Workplan | None:
    wp = db.scalar(crud.get_by_id(id))
    if wp:
        with db.begin_nested():
            wp.retries += 1
            wp.status = Statuses.add

        return wp
