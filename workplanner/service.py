import datetime as dt
from typing import Optional, Union

import peewee
import pendulum
import pydantic

from workplanner import crud, filters
from workplanner import logger
from crud import replay_by_pk
from pbm_helper.workplanner.enums import Statuses, Error
from models import Workplan
from utils import (
    iter_range_datetime,
    iter_period_from_range,
    scroll_to_last_interval_time,
)


def is_create_next(
    name: str,
    interval_timedelta: dt.timedelta,
) -> bool:
    last_executed_item = crud.last(name)
    if last_executed_item is not None:
        result = pendulum.now() - last_executed_item.worktime_utc >= interval_timedelta
        return result
    else:
        result = False

    logger.info("Is create next [{}] {}", name, result)

    return result


def next_worktime(
    name: str, interval_timedelta: dt.timedelta
) -> Optional[pendulum.DateTime]:
    last_item = crud.last(name)

    if last_item is not None:
        return scroll_to_last_interval_time(last_item.worktime_utc, interval_timedelta)

    return None


def create_next_or_none(
    name: str, interval_timedelta: dt.timedelta, *, data: dict = None
) -> Optional["Workplan"]:
    if is_create_next(name, interval_timedelta):
        next_wt = next_worktime(name, interval_timedelta)
        try:
            item = Workplan.create(
                **{
                    **(data or {}),
                    Workplan.name.name: name,
                    Workplan.worktime_utc.name: next_wt,
                }
            )
        except peewee.IntegrityError:
            return None
        else:
            logger.info("Created next workplan [{}] {} {}", name, next_wt, item.id)
            return item


def fill_missing(
    name: str,
    interval_timedelta: dt.timedelta,
    start_time: pendulum.DateTime,
    end_time: pendulum.DateTime = None,
    *,
    data: dict = None,
) -> list["Workplan"]:
    items = []
    end_time = end_time or pendulum.now()

    for wt in iter_range_datetime(start_time, end_time, interval_timedelta):
        item, created = Workplan.get_or_create(
            **{Workplan.name.name: name, Workplan.worktime_utc.name: wt},
            defaults=data or {},
        )
        if created:
            logger.info("Created missing workplans [{}] {} {}", name, wt, item.id)

        items.append(item)

    return items


def recreate_prev(
    name: str,
    offset_periods: Union[pydantic.PositiveInt, list[pydantic.NegativeInt]],
    interval_timedelta: dt.timedelta,
    from_worktime: pendulum.DateTime = None,
    *,
    data: dict = None,
) -> Optional[list["Workplan"]]:

    if isinstance(offset_periods, int):
        if offset_periods > 0:
            offset_periods = [-i for i in range(offset_periods) if i > 0]
        else:
            raise ValueError("Only positive Int")
    else:
        assert all(i < 0 for i in offset_periods)

    first_item = crud.first(name)
    if first_item is not None:
        last_wt = from_worktime or scroll_to_last_interval_time(
            first_item.worktime_utc, interval_timedelta
        )

        worktime_list = [
            last_wt + (interval_timedelta * delta) for delta in offset_periods
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
                interval_timedelta=interval_timedelta,
                start_time=date1,
                end_time=date2,
                data=data,
            )
            items.extend(new_items)

        if worktime_list:
            logger.info("Recreated workplans [{}] {}", name, worktime_list)

        return items

    return None


def is_allowed_execute(name: str, notebook_hash: str, *, max_fatal_errors: int) -> bool:
    item = crud.last(name)

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
            logger.info("Many fatal errors [{}] {}", name, item.id)

        return is_allowed

    return True


def update_errors(name: str, max_retries: int, retry_delay: int) -> peewee.ModelSelect:
    # http://docs.peewee-orm.com/en/latest/peewee/hacks.html?highlight=time%20now#date-math
    # A function that checks to see if retry_delay passes to restart.
    next_start_time_timestamp = Workplan.finished_utc.to_timestamp() + retry_delay
    items = Workplan.select().where(
        Workplan.name == name,
        Workplan.status.in_(Statuses.error_statuses),
        Workplan.retries < max_retries,
        filters.not_expired,
        (
            (pendulum.now().timestamp() >= next_start_time_timestamp)
            | (Workplan.finished_utc.is_null())
        ),
    )
    worktimes = [i.worktime_utc for i in items]

    if worktimes:
        replay_by_pk(name, worktimes=worktimes)
        logger.info("Updated error workplans [{}] {}", name, worktimes)

    return (
        Workplan.select()
        .where(Workplan.name == name, Workplan.worktime_utc.in_(worktimes))
        .order_by(Workplan.worktime_utc.desc())
    )


def iter_generate_child_workplans(
    name: str,
    parent_name: str,
    status_trigger: Statuses.LiteralT,
    *,
    from_worktime: pendulum.DateTime = None,
    data: Optional[dict] = None,
):
    if status_trigger not in Statuses.all_statuses:
        raise ValueError(f"Invalid {status_trigger=}")

    parent_workplans = Workplan.select().where(
        Workplan.name == parent_name, Workplan.status == status_trigger
    )
    if from_worktime:
        parent_workplans = parent_workplans.where(
            Workplan.worktime_utc >= from_worktime
        )

    for parent in parent_workplans:
        parent: Workplan
        item, created = Workplan.get_or_create(
            **{
                Workplan.name.name: name,
                Workplan.worktime_utc.name: parent.worktime_utc,
            },
            defaults=data or {},
        )
        if created:
            logger.info("Created missing workplans [{}] {} {}", name, parent.worktime_utc, item.id)

        yield item


def generate_workplans(  # pylint: disable=R0913
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
) -> peewee.ModelSelect:
    interval_timedelta = dt.timedelta(seconds=interval_in_seconds)

    if parent_name:
        list(
            iter_generate_child_workplans(name, parent_name, status_trigger, data=extra)
        )

    elif is_allowed_execute(
        name, notebook_hash=notebook_hash, max_fatal_errors=max_fatal_errors
    ):
        if not crud.exists(name):
            Workplan.create(
                **{
                    Workplan.name.name: name,
                    Workplan.worktime_utc.name: start_time,
                    **extra,
                }
            )
            logger.info("Created first workplan [{}] {}", name, start_time)

        next_item = create_next_or_none(name, interval_timedelta, data=extra)
        if next_item:
            if back_restarts:
                # When creating the next item,
                # elements are created to update the data for the past dates.
                recreate_prev(
                    name,
                    interval_timedelta=interval_timedelta,
                    offset_periods=back_restarts,
                    data=extra,
                )

        if keep_sequence:
            fill_missing(name, interval_timedelta, start_time, data=extra)

        list(update_errors(name, max_retries, retry_delay))
        check_expiration()

    return (
        Workplan.select()
        .where(Workplan.name == name, *filters.for_executed)
        .order_by(Workplan.worktime_utc.desc())
    )


def clear_statuses_of_lost_items() -> None:
    Workplan.update(**{Workplan.status.name: Statuses.add}).where(
        Workplan.status.in_(Statuses.run_statuses)
    ).execute()


def execute_list(name: str) -> peewee.ModelSelect:
    return (
        Workplan.select()
        .order_by(Workplan.worktime_utc.desc())
        .where(Workplan.name == name, *filters.for_executed)
    )


def check_expiration() -> int:
    return (
        Workplan.update(
            **{Workplan.status.name: Statuses.error, Workplan.info.name: Error.expired}
        )
        .where(filters.expired)
        .execute()
    )
