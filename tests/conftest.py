import uuid

import pendulum
import pytest

from workplanner import database
from workplanner import models


@pytest.fixture(autouse=True)
def preparation_for_tests(tmp_path):
    database.db.database = str(tmp_path.parent / "workplanner-test.db")
    print(f"\nWORKPLANNER_DATABASE_PATH={database.db.database}")
    database.db.connect()
    database.db.create_tables([models.Workplan])
    yield
    models.Workplan.truncate_table()
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
