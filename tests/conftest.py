import os

import orjson
import pendulum
import pytest
import sqlalchemy
from script_master_helper.utils import custom_encoder
from sqlalchemy import orm
from sqlalchemy.orm import Session

from workplanner import const
from workplanner.models import Base

TestSession = orm.scoped_session(
    orm.sessionmaker(autoflush=False, expire_on_commit=False)
)


@pytest.fixture(scope="session", autouse=True)
def homedir(tmp_path_factory):
    dir = tmp_path_factory.getbasetemp()
    os.environ[const.HOME_DIR_VARNAME] = str(dir)
    print(f"\n{const.HOME_DIR_VARNAME}={dir}")

    return dir


@pytest.fixture(scope="session", autouse=True)
def configure_database(homedir):
    db_url = f"sqlite:///{homedir / 'sqlite.db'}"
    print(f"DATABASE URL={db_url}")

    engine = sqlalchemy.create_engine(
        db_url, json_serializer=lambda obj: orjson.dumps(obj, default=custom_encoder)
    )
    # It's a scoped_session, and now is the time to configure it.
    TestSession.configure(bind=engine)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    return engine


@pytest.fixture(scope="function")
def session() -> Session:
    db = TestSession()
    db.begin()
    try:
        yield db
    finally:
        db.rollback()


@pytest.fixture()
def freeze_time() -> pendulum.DateTime:
    now = pendulum.datetime(2022, 11, 11, 11, 11, 11, 0, tz="UTC").start_of("second")
    pendulum.set_test_now(now)
    yield now
    pendulum.set_test_now(now)
