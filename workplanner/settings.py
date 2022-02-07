import os

from confz import ConfZ, ConfZEnvSource, ConfZCLArgSource, ConfZFileSource
from pathlib import Path
from pydantic import validator

import const


class Settings(ConfZ):
    """
    Typer (CLI) replaces "_" characters in parameters with "-", so you need to write parameters without using "_",
    otherwise ConfZ will not see them.

    ConfZ converts ENV variables to lowercase, so lowercase must also be used in the config.
    """
    dbpath: Path
    host: str = const.DEFAULT_HOST
    port: int = const.DEFAULT_PORT
    loglevel: str = const.DEFAULT_LOGLEVEL
    debug: bool = const.DEFAULT_DEBUG

    CONFIG_SOURCES = [
        ConfZEnvSource(allow_all=True, file=const.HOMEPATH() / const.SETTINGS_FILENAME),
        ConfZEnvSource(allow_all=True, prefix="WORKPLANNER_", file=const.HOMEPATH() / const.SETTINGS_FILENAME),
        ConfZFileSource(optional=True, file_from_cl="--settings-file", file_from_env="WORKPLANNER_SETTINGS_FILE"),
        ConfZCLArgSource(),
    ]

    def __init__(self, **kwargs):
        if os.environ.get("PYTEST"):
            kwargs.setdefault("dbpath", "NotImplemented")

        super(Settings, self).__init__(**kwargs)

    @validator("loglevel")
    def validate_loglevel(cls, value):
        if isinstance(value, str):
            return value.upper()
        return value
