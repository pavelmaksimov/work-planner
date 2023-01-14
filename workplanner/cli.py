import os

import typer
from script_master_helper.utils import ProactorServer
from uvicorn import Config

from workplanner import const, __version__

cli = typer.Typer()


@cli.command()
def run(
    # ConfZ automatically reads the CLI parameters from arguments.
    homedir: str = None,
    host: str = const.DEFAULT_HOST,
    port: int = const.DEFAULT_PORT,
    debug: bool = const.DEFAULT_DEBUG,
    loglevel: str = const.DEFAULT_LOGLEVEL,
    logs_rotation: str = const.DEFAULT_LOGLEVEL,
    logs_retention: str = const.DEFAULT_LOGLEVEL,
    database_url: str = None,
    settings_file: str = None,
):
    if homedir:
        os.environ[const.HOME_DIR_VARNAME] = homedir

    from workplanner.database import init_models
    from workplanner.settings import Settings
    from workplanner.app import app

    hello = (
        "...........................................\n"
        f"............ WorkPlanner {__version__} ............\n"
        "...........................................\n"
    )
    typer.echo(typer.style(hello, fg=typer.colors.BRIGHT_YELLOW, bold=True))
    typer.echo(f"Home directory: {const.get_homepath()}")
    typer.echo(f"Swagger: http://{Settings().host}:{Settings().port}/docs")
    typer.echo(f"Api docs: http://{Settings().host}:{Settings().port}/redoc\n")

    for k, v in Settings().dict().items():
        if k == "database_url":
            typer.echo(
                f"DATABASE_URL={Settings().database_url or Settings().default_database_url}"
            )
        else:
            typer.echo(f"{k.upper()}={v}")

    Settings().logdir.mkdir(exist_ok=True)
    init_models()

    config = Config(
        app=app,
        host=Settings().host,
        port=Settings().port,
        reload=Settings().debug,
        log_level=Settings().loglevel.lower(),
        use_colors=True,
    )
    server = ProactorServer(config=config)
    server.run()
