from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel
from script_master_helper.workplanner.client import errors


class HttpErrorDetail(BaseModel):
    message: str
    detail: Any


def get_404_exception(id_or_name):
    return HTTPException(
        errors.not_found_error.code,
        detail=HttpErrorDetail(
            message=errors.not_found_error.message,
            detail=f"Not found workplan: {id_or_name}",
        ),
    )
