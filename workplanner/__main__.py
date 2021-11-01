import os
import pathlib
from functools import partial

import better_exceptions
import typer
import uvicorn

from workplanner import logger

CURRENT_PATH_TO_DB = str(
    os.getenv("WORKPLANNER_DATABASE_PATH", pathlib.Path.home() / "workplanner.db")
)

better_exceptions.hook()
cli = typer.Typer()


@cli.command()
def webserver(
    dbpath: str = CURRENT_PATH_TO_DB,
    host: str = "127.0.0.1",
    port: int = 14444,
    debug: bool = True,
    loglevel: str = "INFO",
):
    os.environ["WORKPLANNER_DATABASE_PATH"] = dbpath
    logger.level(loglevel.upper())

    from workplanner.views import app as webapp

    hello_text = typer.style(
        "\n===== WorkPlanner =====", fg=typer.colors.BRIGHT_YELLOW, bold=True
    )

    typer.echo(hello_text)
    typer.echo(f"Logging level: {loglevel.upper()}")
    typer.echo(f"Debug mode: {debug}")
    typer.echo(f"WORKPLANNER_DATABASE_PATH={dbpath}")
    typer.echo(f"INFO:     http://{host}:{port}/docs")
    typer.echo(f"INFO:     http://{host}:{port}/redoc\n")

    partial(webapp, debug=debug)
    uvicorn.run(
        webapp, host=host, port=port, log_level=loglevel.lower(), use_colors=True
    )


if __name__ == "__main__":
    cli()
