import oracledb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import non_normal_volume
import metadata
import volume

from config import URL_ROOT

oracledb.defaults.config_dir = "."

app = FastAPI(
    title="DVRPC Traffic Counts API",
    description="Please visit [Travel Monitoring Counts](https://www.dvrpc.org/traffic/) for information about the Delaware Valley Regional Planning Commission's traffic counts.",
    version="2.0",
    root_path=f"{URL_ROOT}",
    openapi_url="/openapi.json",
    docs_url="/docs",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(volume.router)
app.include_router(non_normal_volume.router)
app.include_router(metadata.router)
