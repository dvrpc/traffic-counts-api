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
from pydantic.error_wrappers import ValidationError

from config import USER, PASSWORD

oracledb.defaults.config_dir = "."

logger = logging.getLogger(__name__)
logging.basicConfig(filename="api.log", level=logging.DEBUG)

# The field names in the Pydantic models below are the ones in the database.
# They may be changed, to value in `alias`.


# there are 17 counttypes in the TC_COUNTTYPE table. These are grouped below into various
# CountKinds according to their structure (or excluded altogether if not in database, which
# is the NotInDatabaseCountKind), and then grouped into the categories we ulimately want -
# vehicle, bicycle, or pedestrian


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


# "* Day" and "Loop" are currently being recategorized into one of the others in this class
class VehicleCountKind(str, Enum):
    volume = "Volume"
    fifteen_min_volume = "15 min Volume"
    _class = "Class"
    speed = "Speed"
    eight_day = "8 Day"
    loop = "Loop"


# individual counts not in database
class NotInDatabaseCountKind(str, Enum):
    turning_movement = "Turning Movement"
    manual_class = "Manual Class"
    crosswalk = "Crosswalk"


class CountKind(str, Enum):
    vehicle = "vehicle"
    bicycle = "bicycle"
    pedestrian = "pedestrian"
    no_data = "count data not in database"


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
    SOURCE: Optional[str] = Field(alias="source")
    TAKENBY: Optional[str] = Field(alias="taken_by")
    COUNTERID: Optional[str] = Field(alias="counter_id")
    STATIONID: Optional[str] = Field(alias="station_id")
    count_type: Optional[CountKind]
    TYPE: Optional[
        Union[BicycleCountKind, PedestrianCountKind, VehicleCountKind, NotInDatabaseCountKind]
    ] = Field(alias="count_sub_type")
    DESCRIPTION: Optional[str] = Field(alias="description")
    SETDATE: Optional[datetime.date] = Field(alias="set_date")
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
    municipality: Optional[str]
    county: Optional[str]
    state: Optional[str]
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
    AADB: Optional[int]
    AADP: Optional[int]
    AMPEAK: Optional[float] = Field(alias="am_peak")
    AMENDING: Optional[str] = Field(alias="am_ending")
    PMPEAK: Optional[float] = Field(alias="pm_peak")
    PMENDING: Optional[str] = Field(alias="pm_ending")
    COMMENTS: Optional[str] = Field(alias="comments")
    static_pdf: Optional[str]
    counts: List[Count] = []

    # this allows extracting by db field name, but using alias
    class Config:
        allow_population_by_field_name = True


class Error(BaseModel):
    message: str


app = FastAPI(
    title="DVRPC Traffic Counts API",
    description="Please visit [Travel Monitoring Counts](https://www.dvrpc.org/traffic/) for "
    "information about the Delaware Valley Regional Planning Commission's traffic counts.",
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

            try:
                record = Record(**record_data)
            except ValidationError:
                raise

            # map SOURCE to human-readable version
            if record.SOURCE:
                if record.SOURCE == "0":
                    record.SOURCE = "DVRPC"
                elif record.SOURCE == "-1":
                    record.SOURCE = "external"

            # get MCD names
            if record.MCD:
                cursor.execute(
                    "select MCDNAME, COUNTY, STATE from DVRPCTC.TC_MCD where DVRPC = :mcd",
                    mcd=record.MCD,
                )

                columns = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                mcd_data = cursor.fetchone()

                if mcd_data:
                    record.municipality = mcd_data["MCDNAME"]
                    record.county = mcd_data["COUNTY"]
                    record.state = mcd_data["STATE"]

            # Get individual counts of the overall count

            # TC_BIKECOUNT and TC_PEDCOUNT tables have same structure
            if record.TYPE in (
                [each.value for each in BicycleCountKind]
                + [each.value for each in PedestrianCountKind]
            ):
                if record.TYPE in [each.value for each in BicycleCountKind]:
                    tc_table = "DVRPCTC.TC_BIKECOUNT"
                    record.count_type = CountKind.bicycle

                if record.TYPE in [each.value for each in PedestrianCountKind]:
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
                        # do not provide a total if there isn't a full day's count
                        if len(count.items()) != 24:
                            counts[count_date]["total"] = None
                        # otherwise, sum them and put into `counts`
                        else:
                            total = 0
                            for k2, v2 in count.items():
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
            elif record.TYPE in [each.value for each in VehicleCountKind]:
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
            elif record.TYPE in [each.value for each in NotInDatabaseCountKind]:
                # these are not in the database but just in static pdf
                record.count_type = CountKind.no_data
                # the subtype in the url is just the value of TYPE without spaces
                sub_type_in_url = record.TYPE.value.replace(" ", "")
                record.static_pdf = f"https://www.dvrpc.org/asp/TrafficCountPDF/{sub_type_in_url}/{record.RECORDNUM}.PDF"

    return record


@app.get(
    "/api/traffic-counts/v1/records",
    responses=responses,  # type: ignore
    summary="Get record numbers",
)
def get_record_numbers(count_type: Optional[CountKind] = None):
    """
    Get the record numbers of all counts.

    Optionally provide the `count_type` query parameter to get record numbers for specific types of
    counts, e.g. `?count_type=bicycle`.
    """
    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            cursor = connection.cursor()

            if not count_type:
                cursor.execute("select RECORDNUM from DVRPCTC.TC_HEADER")
            else:
                if count_type == CountKind.bicycle:
                    count_types = [each.value for each in BicycleCountKind]
                elif count_type == CountKind.pedestrian:
                    count_types = [each.value for each in PedestrianCountKind]
                elif count_type == CountKind.vehicle:
                    count_types = [each.value for each in VehicleCountKind]
                elif count_type == CountKind.no_data:
                    count_types = [each.value for each in NotInDatabaseCountKind]

                bind_names = ",".join(":" + str(i + 1) for i in range(len(count_types)))
                cursor.execute(
                    f"select RECORDNUM from DVRPCTC.TC_HEADER where type in ({bind_names})",
                    count_types,
                )

            res = cursor.fetchall()

    records = []
    for row in res:
        records.append(row[0])

    return records


@app.get(
    "/api/traffic-counts/v1/record/{num}",
    responses=responses,  # type: ignore
    response_model=Record,
    summary="Get count data in JSON format",
)
def get_record_json(num: int) -> Any:
    try:
        record = get_record(num)
    except ValidationError:
        return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})

    if record is None:
        return JSONResponse(status_code=404, content={"message": "Record not found"})

    return record


@app.get(
    "/api/traffic-counts/v1/record/csv/{num}",
    responses=responses,  # type: ignore
    response_model=Record,
    summary="Get count data in a CSV file",
)
def get_record_csv(num: int) -> Any:
    """
    Metadata will be placed in the first two rows, followed by a blank line, followed by the
    data from the count.
    """
    # create csv/ folder if it doesn't exist
    try:
        Path("csv").mkdir()
    except FileExistsError:
        pass

    csv_file = Path(f"csv/{num}.csv")

    if csv_file.exists():
        return FileResponse(csv_file)

    # otherwise, fetch the data from the database
    try:
        record = get_record(num)
    except ValidationError:
        return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})

    if record is None:
        return JSONResponse(status_code=404, content={"message": "Record not found"})

    # create CSV, save it, return it
    with open(csv_file, "w", newline="") as f:
        # create a writer for the metadata and add it
        fieldnames_metadata = list(Record.schema()["properties"].keys())
        # remove the "counts" field from this - that will be written separately
        fieldnames_metadata.remove("counts")

        writer = csv.DictWriter(f, fieldnames=fieldnames_metadata, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(record.dict(by_alias=True))

        # Create new writer, just to write an empty line to the same file
        writer = csv.writer(f)
        writer.writerow("")

        # create a new writer for the actual count data, and add it to the same file
        fieldnames_count = list(Count.schema()["properties"].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames_count)
        writer.writeheader()
        for count in record.counts:
            writer.writerow(count.dict(by_alias=True))

    return FileResponse(csv_file)
