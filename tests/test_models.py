import datetime as dt

import pendulum
import pytest

from workplanner import crud, service
from workplanner.enums import Statuses


def test_datetime_utc_field(workplan_item):
    workplan_item.started_utc = pendulum.now("Europe/Moscow")
    workplan_item.save()
    assert workplan_item.started_utc == pendulum.now()


def test_filter_by_datetime_utc_field(freeze_today, workplan_item, workplan_model):
    workplan_item.started_utc = pendulum.now("Europe/Minsk")
    workplan_item.save()
    query = workplan_model.select().where(workplan_model.name == workplan_item.name)

    assert query.where(
        workplan_model.started_utc == pendulum.now("Europe/Moscow")
    ).first()
    assert query.where(workplan_model.started_utc == pendulum.now()).first()


@pytest.mark.parametrize(
    "create_retries,max_retries,result", [(0, 0, 0), (0, 1, 1), (1, 1, 0), (1, 0, 0)]
)
def test_retries(create_retries, max_retries, result, freeze_today, workplan_model):
    name = "__test_retries__"
    crud.delete(name=name)
    workplan_model.create(
        **{
            workplan_model.name.name: name,
            workplan_model.worktime_utc.name: freeze_today,
            workplan_model.finished_utc.name: freeze_today,
            workplan_model.status.name: Statuses.error,
            workplan_model.retries.name: create_retries,
        }
    )
    items = service.update_errors(name=name, max_retries=max_retries, retry_delay=0)

    assert len(items) == int(result)


@pytest.mark.parametrize("retry_delay,passed_sec", [(10, 5), (10, 10), (10, 11)])
def test_retry_delay(retry_delay, passed_sec, freeze_today, workplan_model):
    name = "__test_retry_delay__"
    crud.delete(name=name)
    workplan_model.create(
        **{
            workplan_model.name.name: name,
            workplan_model.worktime_utc.name: freeze_today,
            workplan_model.finished_utc.name: freeze_today,
            workplan_model.status.name: Statuses.error,
            workplan_model.retries.name: 0,
        }
    )
    pendulum.set_test_now(freeze_today.add(seconds=passed_sec))
    items = list(service.update_errors(name, max_retries=1, retry_delay=retry_delay))

    assert len(items) == int(passed_sec >= retry_delay)


def test_create_next_execute_item(workplan_model, freeze_utcnow):
    interval_timedelta = dt.timedelta(1)

    item = service.create_next_or_none(
        name=workplan_model.name_for_test,
        interval_timedelta=interval_timedelta,
    )

    assert item is None

    workplan_model.create(
        **{
            workplan_model.name.name: workplan_model.name_for_test,
            workplan_model.worktime_utc.name: freeze_utcnow - interval_timedelta,
        }
    )
    item = service.create_next_or_none(
        name=workplan_model.name_for_test,
        interval_timedelta=interval_timedelta,
    )

    assert item

    item = service.create_next_or_none(
        name=workplan_model.name_for_test,
        interval_timedelta=interval_timedelta,
    )
    assert item is None


def test_create_update_items(workplan_model, freeze_today):
    interval_timedelta = dt.timedelta(1)

    items = service.recreate_prev(
        name=workplan_model.name_for_test,
        from_worktime=freeze_today,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert items is None

    for i in range(10):
        workplan_model.create(
            **{
                workplan_model.name.name: workplan_model.name_for_test,
                workplan_model.worktime_utc.name: freeze_today - dt.timedelta(i),
                workplan_model.status.name: Statuses.success,
            }
        )

    items = service.recreate_prev(
        name=workplan_model.name_for_test,
        from_worktime=freeze_today,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 2

    count = (
        workplan_model.select()
        .where(
            workplan_model.name == workplan_model.name_for_test,
            workplan_model.status == Statuses.add,
        )
        .count()
    )
    assert count == 2
    for i in items:
        assert i.retries == 0


def test_create_update_error_items(workplan_model, freeze_today):
    interval_timedelta = dt.timedelta(1)

    items = service.recreate_prev(
        name=workplan_model.name_for_test,
        from_worktime=freeze_today,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert items is None

    for i in range(10):
        workplan_model.create(
            **{
                workplan_model.name.name: workplan_model.name_for_test,
                workplan_model.worktime_utc.name: freeze_today - dt.timedelta(i),
                workplan_model.status.name: Statuses.error,
            }
        )

    items = service.recreate_prev(
        name=workplan_model.name_for_test,
        from_worktime=freeze_today,
        offset_periods=[-1, -2],
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 2
    count = (
        workplan_model.select()
        .where(
            workplan_model.name == workplan_model.name_for_test,
            workplan_model.status == Statuses.add,
        )
        .count()
    )
    assert count == 2
    for i in items:
        assert i.retries == 0


def test_create_update_items_before_start_time(workplan_model, freeze_today):
    """Checking when the update date is less than the first worktime_utc."""
    interval_timedelta = dt.timedelta(1)
    workplan_model.create(
        **{
            workplan_model.name.name: workplan_model.name_for_test,
            workplan_model.worktime_utc.name: freeze_today - dt.timedelta(1),
            workplan_model.status.name: Statuses.error,
        }
    )
    items = service.recreate_prev(
        name=workplan_model.name_for_test,
        from_worktime=freeze_today,
        offset_periods=3,
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 1


def test_create_update_items_start_time_equals_worktime(workplan_model):
    """Checking when the update date is equals the first worktime_utc."""
    worktime = pendulum.datetime(2020, 1, 1, tz="Europe/Moscow")
    interval_timedelta = dt.timedelta(1)
    workplan_model.create(
        **{
            workplan_model.name.name: workplan_model.name_for_test,
            workplan_model.worktime_utc.name: worktime,
            workplan_model.status.name: Statuses.error,
        }
    )
    items = service.recreate_prev(
        name=workplan_model.name_for_test,
        from_worktime=worktime,
        offset_periods=[-1, -2, -3],
        interval_timedelta=interval_timedelta,
    )

    assert len(items) == 0


def test_create_history_items(workplan_model, freeze_today):
    interval_timedelta = dt.timedelta(1)

    item = service.fill_missing(
        name=workplan_model.name_for_test,
        start_time=freeze_today - dt.timedelta(5),
        end_time=freeze_today,
        interval_timedelta=interval_timedelta,
    )

    assert len(item) == 6


def test_fill_missing_items(workplan_model, freeze_today):
    interval_timedelta = dt.timedelta(1)

    items = service.fill_missing(
        name=workplan_model.name_for_test,
        interval_timedelta=interval_timedelta,
        start_time=freeze_today - dt.timedelta(5),
        end_time=freeze_today - dt.timedelta(5),
    )

    assert len(items) == 1

    workplan_model.create(
        **{
            workplan_model.name.name: workplan_model.name_for_test,
            workplan_model.worktime_utc.name: freeze_today,
        }
    )

    service.fill_missing(
        name=workplan_model.name_for_test,
        start_time=freeze_today - dt.timedelta(5),
        end_time=freeze_today,
        interval_timedelta=interval_timedelta,
    )

    assert (
        workplan_model.select()
        .where(
            workplan_model.name == workplan_model.name_for_test,
            workplan_model.status == Statuses.add,
        )
        .count()
    ) == 6


def test_change_status(workplan_model, freeze_today):
    interval_timedelta = dt.timedelta(1)

    items = service.fill_missing(
        name=workplan_model.name_for_test,
        start_time=freeze_today - dt.timedelta(5),
        end_time=freeze_today,
        interval_timedelta=interval_timedelta,
    )

    crud.set_status(
        workplan_model.name_for_test,
        new_status=Statuses.success,
        from_time_utc=freeze_today - dt.timedelta(5),
        to_time_utc=freeze_today,
    )
    count = (
        workplan_model.select()
        .where(
            workplan_model.name == workplan_model.name_for_test,
            workplan_model.status == Statuses.success,
        )
        .count()
    )
    assert count == len(items)


def test_allow_execute_flow(workplan_model, freeze_today):
    interval_timedelta = dt.timedelta(1)

    for item in service.fill_missing(
        name=workplan_model.name_for_test,
        start_time=freeze_today - dt.timedelta(3),
        end_time=freeze_today,
        interval_timedelta=interval_timedelta,
    ):
        item.status = Statuses.fatal_error
        item.save()

    assert (
        service.is_allowed_execute(
            workplan_model.name_for_test, notebook_hash="", max_fatal_errors=3
        )
        is False
    )
    assert (
        service.is_allowed_execute(
            workplan_model.name_for_test, notebook_hash="new", max_fatal_errors=3
        )
        is True
    )

    service.recreate_prev(
        name=workplan_model.name_for_test,
        from_worktime=freeze_today,
        offset_periods=10,
        interval_timedelta=interval_timedelta,
    )
    assert (
        service.is_allowed_execute(
            workplan_model.name_for_test, notebook_hash="", max_fatal_errors=3
        )
        is True
    )


def test_generate_workplans_without_keep_sequence(workplan_model, freeze_today):
    service.fill_missing(
        workplan_model.name_for_test,
        start_time=freeze_today - dt.timedelta(minutes=4),
        end_time=freeze_today - dt.timedelta(minutes=4),
        interval_timedelta=dt.timedelta(minutes=1),
        data={workplan_model.status.name: Statuses.success},
    )

    items = service.generate_workplans(
        name=workplan_model.name_for_test,
        start_time=freeze_today - dt.timedelta(minutes=10),
        interval_timedelta=dt.timedelta(minutes=1),
        keep_sequence=False,
        max_retries=0,
        retry_delay=0,
        notebook_hash="",
        max_fatal_errors=3,
    )

    assert len(items) == 1


def test_generate_workplans_with_keep_sequence(workplan_model, freeze_today):
    items = service.generate_workplans(
        name=workplan_model.name_for_test,
        start_time=freeze_today - dt.timedelta(minutes=9),
        interval_timedelta=dt.timedelta(minutes=1),
        keep_sequence=True,
        max_retries=2,
        retry_delay=0,
        notebook_hash="",
        max_fatal_errors=1,
    )

    assert len(items) == 10
