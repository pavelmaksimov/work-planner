from contextlib import contextmanager

import orjson
from script_master_helper.utils import custom_encoder
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from workplanner.models import Base
from workplanner.settings import Settings

if not Settings().database_url or "sqlite" in Settings().database_url:
    # For SQlite.
    engine = create_engine(
        Settings().database_url or Settings().default_database_url,
        connect_args={"check_same_thread": False},
        json_serializer=lambda obj: orjson.dumps(obj, default=custom_encoder),
    )
else:
    engine = create_engine(
        Settings().database_url,
        json_serializer=lambda obj: orjson.dumps(obj, default=custom_encoder),
    )

SessionLocal = sessionmaker(engine, autoflush=False, expire_on_commit=False)


def init_models() -> None:
    Base.metadata.create_all(engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def open_session():
    return get_db()
