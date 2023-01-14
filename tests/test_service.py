from datetime import timedelta

import pendulum
import sqlalchemy as sa
from script_master_helper.workplanner.enums import Statuses, Error
from script_master_helper.workplanner.schemas import (
    WorkplanUpdate,
    GenerateWorkplans,
    GenerateChildWorkplans,
)

from tests.factories import WorkplanFactory
from workplanner import crud
from workplanner import service
from workplanner.models import Workplan
from workplanner.utils import iter_range_datetime2


def test_clear_statuses_of_lost_items(session, configure_database):
    freeze_time = pendulum.DateTime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    name = "test_clear_statuses_of_lost_items"
    wp = WorkplanFactory(
        name=name,
        status=Statuses.run,
        retries=3,
        worktime_utc=freeze_time.add(minutes=-1),
        expires_utc=freeze_time,
    )
    WorkplanFactory(
        name=name, status=Statuses.error, retries=3, expires_utc=freeze_time
    )

    items = list(service.clear_statuses_of_lost_items(session))

    assert len(items) == 1
    assert items[0].id == wp.id


def test_update_by_id(session):
    wp = WorkplanFactory()
    schema = WorkplanUpdate(id=wp.id, status=Statuses.queue)
    service.update(session, schema)

    assert session.get(Workplan, wp.pk).status == Statuses.queue


def test_update_by_pk(session):
    wp = WorkplanFactory()
    schema = WorkplanUpdate(
        name=wp.name, worktime_utc=wp.worktime_utc, status=Statuses.queue
    )
    service.update(session, schema)

    assert session.get(Workplan, wp.pk).status == Statuses.queue


def test_many_update(session):
    wp_list = WorkplanFactory.create_many(5)
    WorkplanFactory.create_many(5)

    service.many_update(
        session,
        [
            WorkplanUpdate(
                name=wp_list[0].name,
                worktime_utc=wp_list[0].worktime_utc,
                status=Statuses.queue,
            ),
            WorkplanUpdate(
                name=wp_list[0].name,
                worktime_utc=wp_list[1].worktime_utc,
                status=Statuses.queue,
            ),
        ],
    )
    assert session.execute(
        sa.select(Workplan.id).where(
            Workplan.name == wp_list[0].name, Workplan.status == Statuses.queue
        )
    ).scalars().all() == [wp_list[0].id, wp_list[1].id]


def test_create_by_worktimes(session):
    items = service.create_by_worktimes(
        session,
        "test_create_by_worktimes",
        worktimes=list(iter_range_datetime2(pendulum.now(), 60, 3)),
        data={"status": Statuses.queue},
    )

    assert items


def test_is_create_next(session, freeze_time):
    interval = 60
    name = "test_is_create_next"
    WorkplanFactory.create_many(5, interval, name=name)

    pendulum.set_test_now(freeze_time + timedelta(seconds=interval * 5))

    assert not service.is_create_next(session, name, timedelta(seconds=interval))

    pendulum.set_test_now(freeze_time + timedelta(seconds=interval * 6))

    assert service.is_create_next(session, name, timedelta(seconds=interval))


def test_next_worktime(session, freeze_time):
    interval = 60
    name = "test_next_worktime"
    WorkplanFactory.create_many(5, interval, name=name)

    pendulum.set_test_now(freeze_time + timedelta(seconds=interval * 10))

    next_worktime = service.next_worktime(session, name, timedelta(seconds=interval))

    assert next_worktime == pendulum.now()


def test_create_next_or_none(session, freeze_time):
    interval = 60
    size = 5
    name = "test_create_next_or_none"
    WorkplanFactory.create_many(size, interval, name=name)

    pendulum.set_test_now(freeze_time + timedelta(seconds=interval * size))
    item = service.create_next_or_none(
        session,
        GenerateWorkplans(
            name=name,
            start_time=freeze_time,
            interval_in_seconds=interval,
            extra=GenerateWorkplans.Extra(status=Statuses.queue),
        ),
    )

    assert item is None

    pendulum.set_test_now(freeze_time + timedelta(seconds=interval * 10))
    item = service.create_next_or_none(
        session,
        GenerateWorkplans(
            name=name,
            start_time=freeze_time,
            interval_in_seconds=interval,
            extra=GenerateWorkplans.Extra(status=Statuses.queue),
        ),
    )
    session.commit()
    item = session.scalar(crud.get_by_id(item.id))

    assert item
    assert item.worktime_utc == pendulum.now()
    assert item.status == Statuses.queue


def test_fill_missing(session):
    freeze_time = pendulum.datetime(2022, 1, 1)
    pendulum.set_test_now(freeze_time)
    interval = 60
    name = "test_fill_missing"

    WorkplanFactory(name=name, worktime_utc=freeze_time)
    WorkplanFactory(name=name, worktime_utc=freeze_time.add(seconds=interval))
    WorkplanFactory(name=name, worktime_utc=freeze_time.add(seconds=interval * 2))
    WorkplanFactory(name=name, worktime_utc=freeze_time.add(seconds=interval * 5))

    pendulum.set_test_now(freeze_time.add(seconds=interval * 5))

    items = service.fill_missing(
        session,
        GenerateWorkplans(
            name=name,
            start_time=freeze_time,
            interval_in_seconds=interval,
            extra=GenerateWorkplans.Extra(status=Statuses.queue),
        ),
    )
    worktimes = [wp.worktime_utc for wp in items]

    assert worktimes == [
        freeze_time.add(seconds=interval * 3),
        freeze_time.add(seconds=interval * 4),
    ]


def test_recreate_prev(session):
    freeze_time = pendulum.datetime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)
    size = 5
    interval = 60
    name = "test_recreate_prev"
    wp_list = WorkplanFactory.create_many(size, interval, name=name)
    pendulum.set_test_now(freeze_time + timedelta(seconds=interval * size))

    items = service.recreate_prev(
        session,
        GenerateWorkplans(
            name=name,
            start_time=freeze_time,
            interval_in_seconds=interval,
            back_restarts=2,
            extra=GenerateWorkplans.Extra(status=Statuses.queue),
        ),
    )

    assert tuple(i.worktime_utc for i in items) == (
        wp_list[-2].worktime_utc,
        wp_list[-1].worktime_utc,
    )


def test_recreate_prev2(session):
    freeze_time = pendulum.datetime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)
    size = 5
    interval = 60
    name = "test_recreate_prev"
    wp_list = WorkplanFactory.create_many(size, interval, name=name)
    pendulum.set_test_now(freeze_time + timedelta(seconds=interval * size))

    items = service.recreate_prev(
        session,
        GenerateWorkplans(
            name=name,
            start_time=freeze_time,
            interval_in_seconds=interval,
            back_restarts=[-1, -3],
            extra=GenerateWorkplans.Extra(status=Statuses.queue),
        ),
    )

    assert tuple(i.worktime_utc for i in items) == (
        wp_list[-3].worktime_utc,
        wp_list[-1].worktime_utc,
    )


def test_is_allowed_execute(session):
    freeze_time = pendulum.datetime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    size = 5
    interval = 60
    name = "test_is_allowed_execute"
    WorkplanFactory.create_many(size, interval, name=name, hash="1")
    schema = GenerateWorkplans(
        name=name,
        start_time=freeze_time,
        interval_in_seconds=interval,
        max_fatal_errors=3,
        extra=GenerateWorkplans.Extra(hash="1"),
    )

    assert service.is_allowed_execute(session, schema) is True


def test_is_not_allowed_execute(session):
    freeze_time = pendulum.datetime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    size = 3
    interval = 60
    name = "test_is_not_allowed_execute"
    WorkplanFactory.create_many(
        size, interval, name=name, status=Statuses.fatal_error, hash="1"
    )
    schema1 = GenerateWorkplans(
        name=name,
        start_time=freeze_time,
        interval_in_seconds=interval,
        max_fatal_errors=3,
        extra=GenerateWorkplans.Extra(hash="1"),
    )
    schema2 = GenerateWorkplans(
        name=name,
        start_time=freeze_time,
        interval_in_seconds=interval,
        max_fatal_errors=3,
        extra=GenerateWorkplans.Extra(hash="2"),
    )

    assert service.is_allowed_execute(session, schema1) is False
    assert service.is_allowed_execute(session, schema2) is True


def test_update_errors_max_retries(session):
    freeze_time = pendulum.DateTime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    name = "test_update_errors_max_retries"
    WorkplanFactory(name=name, status=Statuses.error, retries=2)
    WorkplanFactory(
        name=name, status=Statuses.error, retries=3, worktime_utc=freeze_time.add(1)
    )
    WorkplanFactory(
        name=name, status=Statuses.error, retries=4, worktime_utc=freeze_time.add(2)
    )
    WorkplanFactory(
        name=name, status=Statuses.add, retries=0, worktime_utc=freeze_time.add(3)
    )
    schema = GenerateWorkplans(
        name=name,
        start_time=freeze_time,
        interval_in_seconds=60,
        max_fatal_errors=3,
        extra=GenerateWorkplans.Extra(max_retries=3, retry_delay=60),
    )

    items = service.update_errors(session, schema)

    assert len(items) == 1
    assert items[0].retries == 3


def test_update_errors_retry_delay(session):
    freeze_time = pendulum.DateTime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    name = "test_update_errors_retry_delay"
    WorkplanFactory(
        name=name,
        status=Statuses.error,
        retries=0,
        worktime_utc=freeze_time.add(minutes=-1),
        finished_utc=freeze_time,
    )
    WorkplanFactory(
        name=name, status=Statuses.error, retries=0, finished_utc=freeze_time
    )
    schema = GenerateWorkplans(
        name=name,
        start_time=freeze_time,
        interval_in_seconds=60,
        max_fatal_errors=3,
        extra=GenerateWorkplans.Extra(max_retries=3, retry_delay=60),
    )

    items = service.update_errors(session, schema)

    assert not items


def test_update_errors_expired(session):
    freeze_time = pendulum.DateTime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    name = "test_update_errors_expired"
    WorkplanFactory(
        name=name,
        status=Statuses.error,
        retries=3,
        worktime_utc=freeze_time.add(minutes=-1),
    )
    WorkplanFactory(
        name=name, status=Statuses.error, retries=3, expires_utc=freeze_time
    )
    schema = GenerateWorkplans(
        name=name,
        start_time=freeze_time,
        interval_in_seconds=60,
        max_fatal_errors=3,
        retry_delay=0,
        extra=GenerateWorkplans.Extra(max_retries=3),
    )

    items = service.update_errors(session, schema)

    assert not items


def test_execute_list(session):
    freeze_time = pendulum.DateTime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    name = "test_execute_list"
    WorkplanFactory(
        name=name,
        status=Statuses.add,
        worktime_utc=freeze_time.add(minutes=-3),
        expires_utc=freeze_time.add(minutes=-2),
    )
    wp = WorkplanFactory(
        name=name, status=Statuses.add, worktime_utc=freeze_time.add(minutes=-2)
    )
    WorkplanFactory(
        name=name, status=Statuses.run, worktime_utc=freeze_time.add(minutes=-1)
    )

    items = list(service.execute_list(session, name))

    assert len(items) == 1
    assert items[0].id == wp.id


def test_check_expiration(session):
    freeze_time = pendulum.DateTime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    name = "test_check_expiration"
    wp = WorkplanFactory(
        name=name,
        status=Statuses.add,
        worktime_utc=freeze_time.add(minutes=-3),
        expires_utc=freeze_time.add(minutes=-2),
    )
    wp2 = WorkplanFactory(
        name=name,
        status=Statuses.run,
        worktime_utc=freeze_time.add(minutes=-2),
        expires_utc=freeze_time.add(minutes=-1),
    )
    WorkplanFactory(name=name, status=Statuses.run)

    items = list(service.check_expiration(session))

    assert len(items) == 2
    assert sorted((items[0].id, items[1].id)) == sorted((wp.id, wp2.id))
    assert items[0].status == Statuses.error
    assert items[0].info == Error.expired


def test_generate_child_workplans(session):
    freeze_time = pendulum.datetime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    interval = 60
    name = "test_generate_child_workplans"
    WorkplanFactory.create_many(5, interval, name=name, status=Statuses.success)
    name_child = "test_generate_child_workplans_child"
    WorkplanFactory.create_many(3, interval, name=name_child)

    items = service.generate_child_workplans(
        session,
        GenerateChildWorkplans(
            name=name_child, parent_name=name, status_trigger=Statuses.success
        ),
    )

    assert [i.worktime_utc.minute for i in items] == [4, 5]


def test_generate_child_workplans_status_trigger(session):
    freeze_time = pendulum.datetime(2022, 1, 10)
    pendulum.set_test_now(freeze_time)

    interval = 60
    name = "test_generate_child_workplans"
    WorkplanFactory.create_many(5, interval, name=name, status=Statuses.error)
    name_child = "test_generate_child_workplans_child"
    WorkplanFactory.create_many(3, interval, name=name_child)

    items = service.generate_child_workplans(
        session,
        GenerateChildWorkplans(
            name=name_child, parent_name=name, status_trigger=Statuses.success
        ),
    )

    assert not list(items)


def test_generate_workplans_first(session, freeze_time):
    interval = 60
    schema = GenerateWorkplans(
        name="test_generate_workplans_first",
        start_time=freeze_time.add(seconds=-interval * 3),
        interval_in_seconds=interval,
        keep_sequence=False,
        max_fatal_errors=3,
    )
    items = service.generate_workplans(session, schema)
    worktimes = [i.worktime_utc for i in items]

    assert len(worktimes) == 1
    assert worktimes[0] == freeze_time


def test_generate_workplans_fill_missing(session, freeze_time):
    interval = 60
    schema = GenerateWorkplans(
        name="test_generate_workplans_first_and_fill_missing",
        start_time=freeze_time.add(seconds=-interval * 3),
        interval_in_seconds=interval,
        keep_sequence=True,
    )
    items = service.generate_workplans(session, schema)
    worktimes = [i.worktime_utc.minute for i in items]

    assert len(worktimes) == 4
    assert worktimes == [11, 10, 9, 8]


def test_run(session, freeze_time):
    name = "test_run"
    wp = WorkplanFactory(
        name=name, worktime_utc=freeze_time, status=Statuses.fatal_error, retries=1
    )

    item = service.run(session, wp.id)

    assert item.retries == 2
    assert item.status == Statuses.add
