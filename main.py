import oracledb

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from config import USER, PASSWORD

oracledb.defaults.config_dir = "."


class Error(BaseModel):
    message: str


# test3
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="DVRPC Traffic Counts API",
        version="1.0",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app = FastAPI(
    openapi_url="/api/traffic-counts/v1/openapi.json", docs_url="/api/traffic-counts/v1/docs"
)
app.openapi = custom_openapi
responses = {
    400: {"model": Error, "description": "Bad Request"},
    404: {"model": Error, "description": "Not Found"},
    500: {"model": Error, "description": "Internal Server Error"},
}
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get(
    "/api/traffic-counts/v1/records",
    responses=responses,
)
def get_record_nums():
    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            cursor = connection.cursor()
            cursor.execute("select RECORDNUM from DVRPCTC.TC_HEADER")
            res = cursor.fetchall()

    records = []
    for row in res:
        records.append(row[0])

    return records
