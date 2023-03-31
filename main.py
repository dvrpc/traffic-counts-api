import csv
import datetime
from enum import Enum
import logging
from pathlib import Path
from typing import Any, Optional, List, Union

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, FileResponse
import oracledb
from pydantic import BaseModel, Field

from config import USER, PASSWORD

oracledb.defaults.config_dir = "."

logger = logging.getLogger(__name__)
logging.basicConfig(filename="api.log", encoding="utf-8", level=logging.DEBUG)

# The field names in the Pydantic models below are the ones in the database.
# They may be changed, to value in `alias`.


class BicycleCountKind(str, Enum):
    bicycle1 = "Bicycle 1"
    bicycle2 = "Bicycle 2"
    bicycle3 = "Bicycle 3"
    bicycle4 = "Bicycle 4"
    bicycle5 = "Bicycle 5"
    bicycle6 = "Bicycle 6"


class PedestrianCountKind(str, Enum):
    pedestrian = "Pedestrian"
    pedestrian2 = "Pedestrian 2"
    crosswalk = "Crosswalk"


class VehicleCountKind(str, Enum):
    volume = "Volume"


class CountKind(str, Enum):
    vehicle = "vehicle"
    bicycle = "bicycle"
    pedestrian = "pedestrian"


class Count(BaseModel):
    COUNTDATE: datetime.date = Field(alias="date")
    AM12: Optional[int]
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
    PM12: Optional[int]
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
    TOTALCOUNT: Optional[int] = Field(alias="total")
    WEATHER: Optional[str] = Field(alias="weather")
    HIGHTEMP: Optional[str] = Field(alias="high_temp")
    LOWTEMP: Optional[str] = Field(alias="low_temp")

    # this allows extracting by db field name, but using alias
    class Config:
        allow_population_by_field_name = True


class Record(BaseModel):
    RECORDNUM: int = Field(alias="record_num")
    count_type: Optional[CountKind]
    TYPE: Optional[Union[BicycleCountKind, PedestrianCountKind, VehicleCountKind]] = Field(
        alias="count_sub_type"
    )
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
    AMPEAK: Optional[float] = Field(alias="am_peak")
    AMENDING: Optional[str] = Field(alias="am_ending")
    PMPEAK: Optional[float] = Field(alias="pm_peak")
    PMENDING: Optional[str] = Field(alias="pm_ending")
    COMMENTS: Optional[str] = Field(alias="comments")
    counts: List[Count] = []

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


def get_record(num: int) -> Optional[Record]:
    am_pm_map = {
        "00": "AM12",
        "01": "AM1",
        "02": "AM2",
        "03": "AM3",
        "04": "AM4",
        "05": "AM5",
        "06": "AM6",
        "07": "AM7",
        "08": "AM8",
        "09": "AM9",
        "10": "AM10",
        "11": "AM11",
        "12": "PM12",
        "13": "PM1",
        "14": "PM2",
        "15": "PM3",
        "16": "PM4",
        "17": "PM5",
        "18": "PM6",
        "19": "PM7",
        "20": "PM8",
        "21": "PM9",
        "22": "PM10",
        "23": "PM11",
    }

    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            # get overall count metadata
            cursor = connection.cursor()
            cursor.execute("select * from DVRPCTC.TC_HEADER where RECORDNUM = :num", num=num)

            # convert tuple to dictionary
            # <https://python-oracledb.readthedocs.io/en/latest/user_guide/sql_execution.html#rowfactories>
            columns = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            record_data = cursor.fetchone()

            if record_data is None:
                return None

            record = Record(**record_data)

            # Get individual counts of the overall count

            # TC_BIKECOUNT and TC_PEDCOUNT tables have same structure
            if record_data["TYPE"] in (
                [each.value for each in BicycleCountKind]
                + [each.value for each in PedestrianCountKind]
            ):
                if record_data["TYPE"] in [each.value for each in BicycleCountKind]:
                    tc_table = "DVRPCTC.TC_BIKECOUNT"
                    record.count_type = CountKind.bicycle

                if record_data["TYPE"] in [each.value for each in PedestrianCountKind]:
                    tc_table = "DVRPCTC.TC_PEDCOUNT"
                    record.count_type = CountKind.pedestrian

                cursor.execute(
                    f"""
                    SELECT
                        TO_CHAR(COUNTDATE, 'YYYY-MM-DD') as count_date,
                        TO_CHAR(COUNTTIME, 'HH24') as HOUR,
                        SUM(total) AS total
                    FROM {tc_table}
                    WHERE dvrpcnum = :num
                    GROUP BY COUNTDATE, TO_CHAR( COUNTTIME, 'HH24')
                    ORDER BY COUNTDATE, TO_CHAR( COUNTTIME, 'HH24')
                """,
                    num=num,
                )

                columns = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                count_data = cursor.fetchall()

                # that returns a list of dicts in the form
                # {countdate, hour, total}
                # create an intermediate dict of dicts to combine all hours/total by date
                # {date: { am1, am2, ... pm12 ... pm11, total}}

                counts = {}  # type: ignore

                if count_data:
                    for row in count_data:
                        # create new entry if it doesn't yet exist
                        if not counts.get(row["COUNT_DATE"]):
                            counts[row["COUNT_DATE"]] = {}

                        # populate the total by hour
                        for k, v in am_pm_map.items():
                            if row["HOUR"] == k:
                                counts[row["COUNT_DATE"]][v] = row["TOTAL"]

                    # sum total by day, get weather and temps
                    for count_date, count in counts.items():
                        total = 0
                        for k2, v2 in count.items():
                            if type(v2) == int:
                                total += v2
                        counts[count_date]["total"] = total

                        cursor.execute(
                            """
                            SELECT * FROM DVRPCTC.TC_WEATHER
                            WHERE COUNTDATE = TO_DATE(:count_date, 'yyyy-mm-dd')
                        """,
                            count_date=count_date,
                        )

                        columns = [col[0] for col in cursor.description]
                        cursor.rowfactory = lambda *args: dict(zip(columns, args))
                        weather_count = cursor.fetchone()

                        if weather_count:
                            counts[count_date]["weather"] = weather_count["WEATHER"]
                            counts[count_date]["high_temp"] = weather_count["HIGHTEMP"]
                            counts[count_date]["low_temp"] = weather_count["LOWTEMP"]

                # change this dict of dicts into list of Counts and add to record
                record.counts = [Count(COUNTDATE=k, **v) for k, v in counts.items()]

            # TC_VOLCOUNT has a different structure
            # There's no reshaping here because it's already the same as Count
            else:
                record.count_type = CountKind.vehicle
                cursor.execute("select * from DVRPCTC.TC_VOLCOUNT where RECORDNUM = :num", num=num)
                columns = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                count_data = cursor.fetchall()

                if count_data:
                    for row in count_data:
                        cursor.execute(
                            """
                            SELECT * FROM DVRPCTC.TC_WEATHER
                            WHERE COUNTDATE = TO_DATE(:count_date, 'yyyy-mm-dd')
                        """,
                            count_date=row["COUNTDATE"],
                        )

                        columns = [col[0] for col in cursor.description]
                        cursor.rowfactory = lambda *args: dict(zip(columns, args))
                        weather_count = cursor.fetchone()

                        if weather_count:
                            row["weather"] = weather_count["WEATHER"]
                            row["high_temp"] = weather_count["HIGHTEMP"]
                            row["low_temp"] = weather_count["LOWTEMP"]
                        record.counts.append(Count(**row))

    return record


@app.get(
    "/api/traffic-counts/v1/records",
    responses=responses,  # type: ignore
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
    "/api/traffic-counts/v1/record/csv/{num}",
    responses=responses,  # type: ignore
    response_model=Record,
)
def get_record_csv(num: int) -> Any:
    # create csv/ folder if it doesn't exist
    try:
        Path("csv").mkdir()
    except FileExistsError:
        pass

    csv_file = Path(f"csv/{num}.csv")

    # FIXME: uncomment this once the metadata gets written properly to the CSV
    # if csv_file.exists():
    #     return FileResponse(csv_file)

    # otherwise, fetch the data from the database

    record = get_record(num)

    if record is None:
        return JSONResponse(status_code=404, content={"message": "Record not found"})

    # create CSV, save it, return it
    fieldnames = list(Count.schema()["properties"].keys())
    logger.info(fieldnames)
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for count in record.counts:
            writer.writerow(count.dict(by_alias=True))

    return FileResponse(csv_file)


@app.get(
    "/api/traffic-counts/v1/record/{num}",
    responses=responses,  # type: ignore
    response_model=Record,
)
def get_record_json(num: int) -> Any:
    record = get_record(num)

    if record is None:
        return JSONResponse(status_code=404, content={"message": "Record not found"})
    return get_record(num)
