import logging

import oracledb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import classed
import metadata
import non_normal_volume
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
app.include_router(classed.router)
app.include_router(metadata.router)

if __name__ == "main":
    # Create and configure log to be used throughout API. Other modules can access it with
    # `logger = logging.getLogger("api")`.
    logger = logging.getLogger("api")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("traffic_counts_api.log")
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("API starting")
