import logging

from pydantic import BaseModel


class ApiError(Exception):
    pass


class NotFoundError(ApiError):
    pass


class Error(BaseModel):
    message: str


logger = logging.getLogger(__name__)
logging.basicConfig(filename="../fastapi.log", level=logging.DEBUG)


responses = {
    400: {"model": Error, "description": "Bad Request"},
    404: {"model": Error, "description": "Not Found"},
    500: {"model": Error, "description": "Internal Server Error"},
}
