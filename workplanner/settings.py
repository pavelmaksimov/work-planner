from confz import ConfZ, ConfZEnvSource, ConfZCLArgSource, ConfZFileSource
from pydantic import validator

from workplanner import const


class Settings(ConfZ):
    """
    Typer (CLI) replaces "_" characters in parameters with "-", so you need to write parameters without using "_",
    otherwise ConfZ will not see them.

    ConfZ converts ENV variables to lowercase, so lowercase must also be used in the config.
    """

    database_url: str = None
    host: str = const.DEFAULT_HOST
    port: int = const.DEFAULT_PORT
    debug: bool = const.DEFAULT_DEBUG
    loglevel: str = const.DEFAULT_LOGLEVEL
    logs_rotation: str = const.DEFAULT_LOGS_ROTATION
    logs_retention: str = const.DEFAULT_LOGS_RETENTION

    CONFIG_SOURCES = [
        ConfZCLArgSource(),
        ConfZEnvSource(
            allow_all=True, file=const.get_homepath() / const.SETTINGS_FILENAME
        ),
        ConfZEnvSource(
            allow_all=True,
            prefix="WORKPLANNER_",
            file=const.get_homepath() / const.SETTINGS_FILENAME,
        ),
        ConfZFileSource(
            optional=True,
            file_from_cl="--settings-file",
            file_from_env="WORKPLANNER_SETTINGS_FILE",
        ),
    ]

    @property
    def default_database_url(self):
        db_path = const.get_homepath() / "workplanner.db"
        return f"sqlite:///{db_path}"

    @property
    def logpath(self):
        dir = const.get_homepath() / "logs"
        dir.mkdir(exist_ok=True)

        return dir / "workplanner.log"

    def __init__(self, **kwargs):
        super(Settings, self).__init__(**kwargs)

    @validator("loglevel")
    def validate_loglevel(cls, value):
        if isinstance(value, str):
            return value.upper()
        return value
