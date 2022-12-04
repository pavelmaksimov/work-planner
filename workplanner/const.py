import os

from pathlib import Path

SETTINGS_FILENAME = ".env"
HOME_DIR_VARNAME = "WORKPLANNER_HOME"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 14444
DEFAULT_LOGLEVEL = "INFO"
DEFAULT_LOGS_ROTATION = "1 day"  # Once the file is too old, it's rotated
DEFAULT_LOGS_RETENTION = "1 months"  # Cleanup after some time
DEFAULT_DEBUG = False
DEFAULT_PROCESS_TIME_LIMIT = 3600
DEFAULT_PROCESS_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 60


def get_homepath() -> Path:
    try:
        return Path(os.environ[HOME_DIR_VARNAME])
    except KeyError:
        if os.environ.get("PYTEST") or Path().cwd().name == "tests":
            return Path("NotImplemented")
        elif Path(".env") in Path().glob("*"):
            return Path().cwd()

        raise
