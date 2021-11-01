"""
https://fastapi.tiangolo.com/advanced/sql-databases-peewee/
"""
import os
import pathlib
from contextvars import ContextVar

import peewee

db_state_default = {"closed": None, "conn": None, "ctx": None, "transactions": None}
db_state = ContextVar("db_state", default=db_state_default.copy())


class PeeweeConnectionState(peewee._ConnectionState):  # pylint: disable=C0321
    def __init__(self, **kwargs):
        super().__setattr__("_state", db_state)
        super().__init__(**kwargs)

    def __setattr__(self, name, value):
        self._state.get()[name] = value

    def __getattr__(self, name):
        return self._state.get()[name]


path_to_db = os.getenv(
    "WORKPLANNER_DATABASE_PATH", pathlib.Path.home() / "workplanner.db"
)
db = peewee.SqliteDatabase(path_to_db, check_same_thread=False)
db._state = PeeweeConnectionState()  # pylint: disable=C0321
