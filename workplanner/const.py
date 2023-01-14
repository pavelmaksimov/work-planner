import os

from pathlib import Path

SETTINGS_FILENAME = ".env"
HOME_DIR_VARNAME = "WORKPLANNER_HOME"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8081
DEFAULT_LOGLEVEL = "INFO"
DEFAULT_LOGS_ROTATION = "1 day"  # Once the file is too old, it's rotated
DEFAULT_LOGS_RETENTION = "1 months"  # Cleanup after some time
DEFAULT_DEBUG = False


def get_homepath() -> Path:
    try:
        return Path(os.environ[HOME_DIR_VARNAME])
    except KeyError:
        if os.environ.get("PYTEST") or Path().cwd().name == "tests":
            return Path("NotImplemented")
        elif Path(SETTINGS_FILENAME) in Path().glob("*"):
            return Path().cwd()

        raise
