import datetime as dt
from typing import Optional, Union

import peewee
import pendulum
import pydantic
from loguru import logger

from work_planner import crud, filters
from work_planner.enums import Statuses, Error
from work_planner.models import Workplan
from work_planner.utils import iter_range_datetime, iter_period_from_range


def beat() -> None:
    check_expiration()


def is_create_next(
    name: str,
    interval_timedelta: dt.timedelta,
    worktime_utc: pendulum.DateTime,
) -> bool:
    last_executed_item = crud.last_worktime(name)
    return (
        last_executed_item
        and worktime_utc - last_executed_item.worktime_utc >= interval_timedelta
    )


def next_worktime_utc(name: str, interval_timedelta: dt.timedelta) -> pendulum.DateTime:
    last_executed_item = crud.last_worktime(name)
    return last_executed_item.worktime_utc + interval_timedelta


def create_next(
    name: str,
    interval_timedelta: dt.timedelta,
    worktime_utc: pendulum.DateTime,
) -> Optional["Workplan"]:
    if is_create_next(name, interval_timedelta, worktime_utc):
        last_executed_item = crud.last_worktime(name)
        next_worktime = last_executed_item.worktime_utc + interval_timedelta
        try:
            item = Workplan.create(
                **{
                    Workplan.name.name: name,
                    Workplan.worktime_utc.name: next_worktime,
                }
            )
        except peewee.IntegrityError:
            return None
        else:
            logger.info("Created next worktime_utc {} for {}", next_worktime, name)
            return item


def fill_missing(
    name: str,
    start_time: pendulum.DateTime,
    end_time: pendulum.DateTime,
    interval_timedelta: dt.timedelta,
    **kwargs
) -> list["Workplan"]:
    items = []
    print("fill_missing__", start_time, end_time, interval_timedelta)
    for worktime in iter_range_datetime(start_time, end_time, interval_timedelta):
        print("worktime__", worktime)
        try:
            item = Workplan.create(
                **{
                    **kwargs,
                    Workplan.name.name: name,
                    Workplan.worktime_utc.name: worktime,
                }
            )
        except peewee.IntegrityError:
            item = Workplan.get(
                Workplan.name == name, Workplan.worktime_utc == worktime
            )
        else:
            logger.info("Created missing workplan {} for {}", worktime, name)

        items.append(item)

    return items


def recreate_prev(
    name: str,
    worktime: pendulum.DateTime,
    offset_periods: Union[pydantic.PositiveInt, list[pydantic.NegativeInt]],
    interval_timedelta: dt.timedelta,
) -> Optional[list["Workplan"]]:

    if isinstance(offset_periods, int):
        if offset_periods > 0:
            offset_periods = [-i for i in range(offset_periods) if i > 0]
        else:
            raise ValueError("Only positive Int")
    else:
        assert all([i < 0 for i in offset_periods])

    first_item = crud.first(name)
    if first_item:
        worktime_list = [
            worktime + (interval_timedelta * delta) for delta in offset_periods
        ]
        worktime_list = list(
            filter(lambda dt_: dt_ >= first_item.worktime_utc, worktime_list)
        )

        Workplan.delete().where(
            Workplan.name == name, Workplan.worktime_utc.in_(worktime_list)
        ).execute()

        items = []
        for date1, date2 in iter_period_from_range(worktime_list, interval_timedelta):
            new_items = fill_missing(
                name,
                start_time=date1,
                end_time=date2,
                interval_timedelta=interval_timedelta,
            )
            items.extend(new_items)

        logger.info(
            "Recreated items to restart flows {} for previous worktimes {}",
            name,
            worktime_list,
        )

        return items


def is_allowed_execute(name: str, notebook_hash: str, *, max_fatal_errors: int) -> bool:
    item = crud.last_worktime(name)

    if item and item.hash == notebook_hash:
        # Check limit fatal errors.
        items = (
            Workplan.select()
            .where(Workplan.name == name, Workplan.status == Statuses.fatal_error)
            .order_by(Workplan.updated_utc.desc())
            .limit(max_fatal_errors)
        )
        is_allowed = len(items) < max_fatal_errors
        if not is_allowed:
            logger.info("Many fatal errors, {} will not be scheduled", name)

        return is_allowed
    else:
        return True


def check_expiration() -> int:
    return Workplan.update(
        **{Workplan.status.name: Statuses.error, Workplan.info.name: Error.expired}
    ).where(filters.expired)


def retry_error_items(name: str, retries: int, retry_delay: int) -> peewee.ModelSelect:
    # http://docs.peewee-orm.com/en/latest/peewee/hacks.html?highlight=time%20now#date-math
    # A function that checks to see if retry_delay passes to restart.
    next_start_time_timestamp = Workplan.finished_utc.to_timestamp() + retry_delay
    items = Workplan.select().where(
        Workplan.name == name,
        Workplan.status.in_(Statuses.error_statuses),
        Workplan.retries < retries,
        filters.not_expired,
        (
            (pendulum.now("UTC").timestamp() >= next_start_time_timestamp)
            | (Workplan.finished_utc.is_null())
        ),
    )
    worktimes = [i.worktime_utc for i in items]

    if worktimes:
        Workplan.update(
            **{
                Workplan.status.name: Statuses.add,
                Workplan.retries.name: Workplan.retries + 1,
                Workplan.info.name: None,
                Workplan.updated_utc.name: pendulum.now("UTC"),
            }
        ).where(Workplan.name == name, Workplan.worktime_utc.in_(worktimes)).execute()

        logger.info("Restart error items for {}, worktimes = {}", name, worktimes)

    return (
        Workplan.select()
        .where(Workplan.name == name, Workplan.worktime_utc.in_(worktimes))
        .order_by(Workplan.worktime_utc.desc())
    )


def get_items_for_execute(
    name: str,
    worktime: pendulum.DateTime,
    start_time: pendulum.DateTime,
    interval_timedelta: dt.timedelta,
    keep_sequence: bool,
    retries: int,
    retry_delay: int,
    notebook_hash: str,
    max_fatal_errors: int,
    update_stale_data: Optional[
        Union[pydantic.PositiveInt, list[pydantic.NegativeInt]]
    ] = None,
) -> peewee.ModelSelect:
    if is_allowed_execute(
        name, notebook_hash=notebook_hash, max_fatal_errors=max_fatal_errors
    ):
        if not crud.exists(name):
            Workplan.create(
                **{Workplan.name.name: name, Workplan.worktime_utc.name: start_time}
            )
            logger.info("Created first item for {}, worktime_utc {}", name, start_time)

        if create_next(name, interval_timedelta, worktime):
            if update_stale_data:
                # When creating the next item, elements are created to update the data for the past dates.
                recreate_prev(name, worktime, update_stale_data, interval_timedelta)

        if keep_sequence:
            fill_missing(name, start_time, worktime, interval_timedelta)

        retry_error_items(name, retries, retry_delay)

    return (
        Workplan.select()
        .where(
            Workplan.name == name,
            Workplan.status == Statuses.add,
            filters.not_expired,
        )
        .order_by(Workplan.worktime_utc.desc())
    )


def clear_statuses_of_lost_items() -> None:
    Workplan.update(**{Workplan.status.name: Statuses.add}).where(
        Workplan.status.in_([Statuses.run])
    ).execute()


def execute_list(name: str) -> peewee.ModelSelect:
    return (
        Workplan.select()
        .order_by(Workplan.worktime_utc.desc())
        .where(Workplan.name == name, Workplan.status == Statuses.add)
    )
