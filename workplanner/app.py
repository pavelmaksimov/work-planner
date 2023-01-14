import os
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import ORJSONResponse
from loguru import logger
from script_master_helper.workplanner import schemas
from script_master_helper.workplanner.schemas import Error
from starlette import status
from starlette.requests import Request
from starlette.responses import Response

from workplanner import errors, service
from workplanner.database import open_session
from workplanner.resources import router, API_VERSION
from workplanner.settings import Settings

fmt = os.environ.get(
    "LOGURU_FORMAT",
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
    "<level>{message}</level>",
)

if os.environ.get("PYTEST") or Path().cwd().name == "tests":
    logger.configure(
        handlers=[
            {
                "sink": sys.stdout,
                "level": "DEBUG",
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
                "level": Settings().loglevel,
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

app = FastAPI(version=API_VERSION, title="WorkPlanner", debug=Settings().debug)
app.include_router(router)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    try:
        response: Response = await call_next(request)
    except Exception:
        logger.exception("{} {}", request.method, request.url)
        raise

    if response.status_code == 200:
        logger.info("{} {} - {}", request.method, request.url, response.status_code)
    else:
        logger.error("{} {} - {}", request.method, request.url, response.status_code)

    return response


@app.exception_handler(Exception)
async def exception_handler(request: Request, exc):
    logger.exception(str(exc))
    return ORJSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=jsonable_encoder(
            schemas.ResponseGeneric(
                error=Error(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    message="Internal server error",
                )
            )
        ),
    )


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request, exc: HTTPException):
    if not isinstance(exc.detail, errors.HttpErrorDetail):
        logger.exception("{} {} - {}", request.method, request.url, exc.status_code)
        raise exc

    logger.error(repr(exc))

    return ORJSONResponse(
        status_code=exc.status_code,
        content=jsonable_encoder(
            schemas.ResponseGeneric(
                error=Error(
                    code=exc.status_code,
                    message=exc.detail.message,
                    detail=exc.detail.detail,
                )
            )
        ),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.error("{}\nbody={}", exc, exc.body)
    return ORJSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder(
            schemas.ResponseGeneric(
                error=Error(
                    code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    message="Request validation error",
                    detail=exc.errors(),
                )
            )
        ),
    )


@app.on_event("startup")
def startup():
    with open_session() as s:
        service.clear_statuses_of_lost_items(s)


@app.on_event("shutdown")
def shutdown():
    with open_session() as s:
        service.clear_statuses_of_lost_items(s)
