from pydantic import BaseModel


class ApiError(Exception):
    pass


class NotFoundError(ApiError):
    pass


class Error(BaseModel):
    message: str


responses = {
    400: {"model": Error, "description": "Bad Request"},
    404: {"model": Error, "description": "Not Found"},
    500: {"model": Error, "description": "Internal Server Error"},
}
