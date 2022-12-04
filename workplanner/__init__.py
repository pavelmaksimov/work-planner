import os
import sys
from pathlib import Path

from loguru import logger

from workplanner import const
from workplanner.settings import Settings

fmt = os.environ.get(
    "LOGURU_FORMAT",
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
)

if os.environ.get("PYTEST") or Path().cwd().name == "tests":
    logger.configure(
        handlers=[
            {
                "sink": sys.stdout,
                "colorize": True,
                "backtrace": True,
                "diagnose": True,
                "format": fmt,
            }
        ]
    )
else:
    logger.configure(
        handlers=[
            {
                "sink": sys.stdout,
                "colorize": True,
                "backtrace": True,
                "diagnose": True,
                "format": fmt,
            },
            {
                "sink": Settings().logpath,
                "format": fmt,
                "rotation": Settings().logs_rotation,
                "retention": Settings().logs_retention,
                "compression": "zip",
                "colorize": False,
                "enqueue": True,
                "backtrace": True,
                "diagnose": True,
            },
        ]
    )

__version__ = "0.0.1"
__all__ = ["logger", "__version__"]
