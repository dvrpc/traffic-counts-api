from fastapi.responses import JSONResponse
from pydantic import BaseModel


class ApiError(Exception):
    pass


class NotFoundError(ApiError):
    status_code = 404
    message = "Record not found."
    json = JSONResponse(status_code=404, content={"message": message})


class NotPublishedError(ApiError):
    status_code = 403
    message = "Requested record is not published."
    json = JSONResponse(status_code=status_code, content={"message": message})


class Error(BaseModel):
    message: str


responses = {
    400: {"model": Error, "description": "Bad Request"},
    404: {"model": Error, "description": "Not Found"},
    500: {"model": Error, "description": "Internal Server Error"},
}
