import better_exceptions
import os
import typer
from functools import partial
from uvicorn import Config

import const
from pbm_helper.utils import ProactorServer
from settings import Settings

better_exceptions.hook()
cli = typer.Typer()


@cli.command()
def main(
        homedir: str = None,
        host: str = const.DEFAULT_HOST,
        port: int = const.DEFAULT_PORT,
        debug: bool = const.DEFAULT_DEBUG,
        loglevel: str = const.DEFAULT_LOGLEVEL,
        dbpath: str = None,
        settings_file: str = None,
):
    # ConfZ automatically reads the CLI parameters.

    if homedir:
        os.environ[const.HOME_DIR_VARNAME] = homedir

    hello_text = typer.style(
        "\n===== WorkPlanner =====", fg=typer.colors.BRIGHT_YELLOW, bold=True
    )
    typer.echo(hello_text)

    for k, v in Settings().dict().items():
        typer.echo(f"{k.upper()}: {v}")

    typer.echo(f"Swagger: http://{Settings().host}:{Settings().port}/docs")
    typer.echo(f"Api docs: http://{Settings().host}:{Settings().port}/redoc\n")

    from views import app

    partial(app, debug=Settings().debug)

    config = Config(
        app=app,
        host=Settings().host,
        port=Settings().port,
        reload=Settings().debug,
        debug=Settings().debug,
        log_level=Settings().loglevel.lower(),
        use_colors=True,
    )
    server = ProactorServer(config=config)
    server.run()


if __name__ == "__main__":
    typer.run(main)
