import datetime
import logging
from typing import Any, Optional, List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
import oracledb
from pydantic import BaseModel, Field, validator

from config import USER, PASSWORD

oracledb.defaults.config_dir = "."


logger = logging.getLogger(__name__)
logging.basicConfig(filename="api.log", encoding="utf-8", level=logging.DEBUG)



# The field names in the Pydantic models below are the ones in the database.
# They may be changed, to value in `alias`.

class VehicleCount(BaseModel):
    COUNTDATE: datetime.date = Field(alias="date")
    TOTALCOUNT: Optional[int] = Field(alias="total")
    WEATHER: Optional[str] = Field(alias="weather")
    AM1: Optional[int]
    AM2: Optional[int]
    AM3: Optional[int]
    AM4: Optional[int]
    AM5: Optional[int]
    AM6: Optional[int]
    AM7: Optional[int]
    AM8: Optional[int]
    AM9: Optional[int]
    AM10: Optional[int]
    AM11: Optional[int]
    AM12: Optional[int]
    PM1: Optional[int]
    PM2: Optional[int]
    PM3: Optional[int]
    PM4: Optional[int]
    PM5: Optional[int]
    PM6: Optional[int]
    PM7: Optional[int]
    PM8: Optional[int]
    PM9: Optional[int]
    PM10: Optional[int]
    PM11: Optional[int]
    PM12: Optional[int]

    # this allows extracting by db field name, but using alias
    class Config:
        allow_population_by_field_name = True

class PedestrianCount(BaseModel):
    COUNTDATE: datetime.date = Field(alias="date")
    COUNTTIME: datetime.datetime = Field(alias="time")
    INCOUNT: Optional[int] = Field(alias="in_count")
    OUTCOUNT: Optional[int] = Field(alias="out_count")
    TOTAL: Optional[int] = Field(alias="total")

    # convert bad year:correct time to just time
    # (the database uses a date field but no date is included and so it defaults to 1899)
    @validator('COUNTTIME')
    def datetime_to_time(cls,v):
        return v.time()

    # this allows extracting by db field name, but using alias
    class Config:
        allow_population_by_field_name = True
        

class BicycleCount(BaseModel):
    COUNTDATE: datetime.date = Field(alias="date")
    COUNTTIME: datetime.datetime = Field(alias="time")
    COUNT_MON: Optional[int] = Field(alias="month")
    COUNT_WEEKDAY: Optional[int] = Field(alias="weekday")
    COUNT_YR: Optional[int] = Field(alias="year")
    INCOUNT: Optional[int] = Field(alias="in_count")
    OUTCOUNT: Optional[int] = Field(alias="out_count")
    TOTAL: Optional[int] = Field(alias="total")

    # convert bad year:correct time to just time
    # (the database uses a date field but no date is included and so it defaults to 1899)
    @validator('COUNTTIME')
    def datetime_to_time(cls,v):
        return v.time()
    # this allows extracting by db field name, but using alias
    class Config:
        allow_population_by_field_name = True

class Record(BaseModel):
    RECORDNUM: int = Field(alias="record_num")
    TYPE: Optional[str] = Field(alias="type")
    SETDATE: Optional[datetime.date] = Field(alias="date")
    TAKENBY: Optional[str] = Field(alias="taken_by")
    COUNTERID: Optional[str] = Field(alias="counter_id")
    STATIONID: Optional[str] = Field(alias="station_id")
    DESCRIPTION: Optional[str] = Field(alias="description")
    PRJ: Optional[str] = Field(alias="project")
    PROGRAM: Optional[str] = Field(alias="program")
    BIKEPEDGROUP: Optional[str] = Field(alias="group")
    BIKEPEDFACILITY: Optional[str] = Field(alias="facility")
    SR: Optional[str]
    SEQ: Optional[str]
    OFFSET: Optional[str] = Field(alias="offset")
    SRI: Optional[str]
    MP: Optional[str]
    LATITUDE: Optional[float] = Field(alias="latitude")
    LONGITUDE: Optional[float] = Field(alias="longitude")
    MCD: Optional[str]
    ROUTE: Optional[int] = Field(alias="route")
    ROAD: Optional[str] = Field(alias="road")
    RDPREFIX: Optional[str] = Field(alias="road_prefix")
    RDSUFFIX: Optional[str] = Field(alias="road_suffix")
    ISURBAN: Optional[str] = Field(alias="is_urban")
    SIDEWALK: Optional[str] = Field(alias="sidewalk")
    OUTDIR: Optional[str] = Field(alias="out_direction")
    INDIR: Optional[str] = Field(alias="in_direction")
    CNTDIR: Optional[str] = Field(alias="counter_direction")
    TRAFDIR: Optional[str] = Field(alias="traffic_direction")
    FC: Optional[int]
    SPEEDLIMIT: Optional[int] = Field(alias="speed_limit")
    WEATHER: Optional[str] = Field(alias="weather")
    AXLE: Optional[float]
    FACTOR: Optional[float]
    AADT: Optional[int]
    AMPEAK: Optional[float]
    AMENDING:Optional[str]
    PMPEAK: Optional[float]
    PMENDING: Optional[str]
    COMMENTS: Optional[str] = Field(alias="comments")
    # the type on counts is handled in queries, because Pydantic
    # or this programmer isn't smart enough to figure out how to coerce into 
    # correct one properly
    counts: Any

    # this allows extracting by db field name, but using alias
    class Config:
        allow_population_by_field_name = True


class Error(BaseModel):
    message: str


app = FastAPI(
    title="DVRPC Traffic Counts API",
    version="1.0",
    openapi_url="/api/traffic-counts/v1/openapi.json", 
    docs_url="/api/traffic-counts/v1/docs",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

responses = {
    400: {"model": Error, "description": "Bad Request"},
    404: {"model": Error, "description": "Not Found"},
    500: {"model": Error, "description": "Internal Server Error"},
}


@app.get(
    "/api/traffic-counts/v1/records",
    responses=responses, # type: ignore
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


@app.get(
    "/api/traffic-counts/v1/record/{num}",
    responses=responses, # type: ignore
    response_model=Record,
)
def get_record(num: int) -> Any:

    # These are all the types in the TC_COUNTTYPE table. 
    # Not yet sure exactly how they can be grouped.
    """
    15 min Volume
    8 Day
    Bicycle 1
    Bicycle 2
    Bicycle 3
    Bicycle 4
    Bicycle 5
    Bicycle 6
    Class
    Crosswalk
    Loop
    Manual Class
    Pedestrian
    Pedestrian 2
    Speed
    Turning Movement
    Volume
    """
    
    bike_counts = [
        "Bicycle 1",
        "Bicycle 2",
        "Bicycle 3",
        "Bicycle 4",
        "Bicycle 5",
        "Bicycle 6",
    ]

    ped_counts = ["Pedestrian", "Pedestrian 2"]
   
    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            # get overall count metadata
            cursor = connection.cursor()
            cursor.execute("select * from DVRPCTC.TC_HEADER where RECORDNUM = :num", num=num)

            # convert tuple to dictionary
            # <https://python-oracledb.readthedocs.io/en/latest/user_guide/sql_execution.html#rowfactories>
            columns = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            record = cursor.fetchone()

            counts = []

            # set table according to what type of overall count this is
            # NOTE: BIKE and PED use "DVRPCNUM" for the id rather than "RECORDNUM"
            if record["TYPE"] in bike_counts:
                cursor.execute("select * from DVRPCTC.TC_BIKECOUNT where DVRPCNUM = :num", num=num)

                # get individual counts
                columns = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                count = cursor.fetchall()
                if count:
                    for row in count:
                        counts.append(BicycleCount(**row))

            elif record["TYPE"] in ped_counts:
                cursor.execute("select * from DVRPCTC.TC_PEDCOUNT where DVRPCNUM = :num", num=num)

                # get individual counts
                columns = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                count = cursor.fetchall()
                if count:
                    for row in count:
                        counts.append(PedestrianCount(**row))  # type: ignore
            else:
                cursor.execute("select * from DVRPCTC.TC_VOLCOUNT where RECORDNUM = :num", num=num)
                
                # get individual counts
                columns = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                count = cursor.fetchall()
                if count:
                    for row in count:
                        counts.append(VehicleCount(**row))  # type: ignore

    if record is None:
        return JSONResponse(status_code=404, content={"message": "Record not found"})

    # add the volume counts
    record["counts"] = counts

    return record
