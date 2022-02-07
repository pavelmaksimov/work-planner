import os

import uuid

import pendulum
import pytest

import database, const
import models


def pytest_sessionstart():
    pass


def pytest_unconfigure():
    pass


@pytest.fixture(autouse=True)
def before_test(tmp_path):
    os.environ[const.HOME_DIR_VARNAME] = str(tmp_path.parent)

    database.db.database = str(tmp_path.parent / "workplanner-test.db")
    print(f"\nDATABASE-PATH={database.db.database}")
    database.db.connect()
    database.db.create_tables([models.Workplan])
    yield
    database.db.close()


@pytest.fixture()
def freeze_today():
    now = pendulum.datetime(2021, 1, 1, tz="UTC")
    pendulum.set_test_now(now)
    yield now
    pendulum.set_test_now(now)


@pytest.fixture()
def freeze_utcnow():
    now = pendulum.datetime(2021, 1, 1, 10, 10, 10, tz="UTC")
    pendulum.set_test_now(now)
    yield now
    pendulum.set_test_now(now)


@pytest.fixture()
def workplan_model():
    models.Workplan.name_for_test = str(uuid.uuid4())
    yield models.Workplan


@pytest.fixture()
def workplan_item(freeze_today):
    yield models.Workplan.create(
        **{
            models.Workplan.name.name: str(uuid.uuid4()),
            models.Workplan.worktime_utc.name: freeze_today,
        }
    )
