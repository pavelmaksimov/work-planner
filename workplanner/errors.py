from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel


class HttpErrorDetail(BaseModel):
    message: str
    detail: Any


def get_404_exception(id_or_name):
    return HTTPException(
        404,
        detail=HttpErrorDetail(
            message="Object not found",
            detail=f"Not found workplan: {id_or_name}",
        ),
    )
