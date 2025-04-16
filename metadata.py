import datetime
from typing import Optional, Union

import oracledb
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import AliasGenerator, BaseModel, Field, ValidationError

from common import responses
from config import PASSWORD, USER
from counts import (
    BicycleCountKind,
    CountKind,
    NotInDatabaseCountKind,
    PedestrianCountKind,
    VehicleCountKind,
)


class Metadata(BaseModel):
    """
    Count metadata, from tc_header table.
    """

    # The field names on the left are the ones in the database, which have been aliased
    # to be more informative/user-friendly.

    RECORDNUM: int = Field(alias="record_num")
    SOURCE: Optional[str]
    COUNTERID: Optional[str] = Field(alias="counter_id")
    STATIONID: Optional[str] = Field(alias="station_id")
    count_type: Optional[CountKind] = None
    TYPE: Optional[
        Union[BicycleCountKind, PedestrianCountKind, VehicleCountKind, NotInDatabaseCountKind]
    ] = Field(alias="count_type")
    SETDATE: Optional[datetime.date] = Field(alias="set_date")
    PRJ: Optional[str] = Field(alias="project")
    PROGRAM: Optional[str]
    BIKEPEDGROUP: Optional[str] = Field(alias="group")
    BIKEPEDFACILITY: Optional[str] = Field(alias="facility")
    SR: Optional[str]
    SEG: Optional[str]
    OFFSET: Optional[str]
    SRI: Optional[str]
    MP: Optional[str]
    LATITUDE: Optional[float]
    LONGITUDE: Optional[float]
    MCD: Optional[str]
    municipality: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None
    ROUTE: Optional[int]
    ROAD: Optional[str]
    RDPREFIX: Optional[str] = Field(alias="road_prefix")
    RDSUFFIX: Optional[str] = Field(alias="road_suffix")
    ISURBAN: Optional[str] = Field(alias="is_urban")
    SIDEWALK: Optional[str]
    CLDIR1: Optional[str] = Field(alias="lane1_dir")
    CLDIR2: Optional[str] = Field(alias="lane2_dir")
    CLDIR3: Optional[str] = Field(alias="lane3_dir")
    CNTDIR: Optional[str] = Field(alias="count_direction")
    TRAFDIR: Optional[str] = Field(alias="traffic_direction")
    FC: Optional[int] = Field(alias="functional_class")
    SPEEDLIMIT: Optional[int] = Field(alias="speed_limit")
    AADV: Optional[int]
    AM_PEAK_VOLUME: Optional[int]
    AVG_AM_MAX_PERCENT: Optional[float]
    PM_PEAK_VOLUME: Optional[int]
    AVG_PM_MAX_PERCENT: Optional[float]
    COMMENTS: Optional[str] = Field(alias="comments")

    class Config:
        validate_by_name = True
        alias_generator = AliasGenerator(
            serialization_alias=lambda field_name: field_name.lower(),
        )


router = APIRouter()


@router.get(
    "/records",
    responses=responses,
    summary="Get count numbers, optionally by type/subtype, in JSON format",
)
def get_count_numbers(
    count_type: Optional[CountKind] = None, sub_type: Optional[VehicleCountKind] = None
) -> list[int]:
    """
    Get the record numbers of all counts, in descending order.

    Optionally refine by count type or by subtype (for vehicles) with query parameters.
    For example: `?count_type=vehicle&sub_type=Class` or `?count_type=bicycle`.
    """
    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            cursor = connection.cursor()

            if not count_type and not sub_type:
                cursor.execute("select recordnum from tc_header")
            else:
                if sub_type:
                    count_types = [sub_type]
                elif count_type == CountKind.vehicle:
                    count_types = [each.value for each in VehicleCountKind]
                elif count_type == CountKind.bicycle:
                    count_types = [each.value for each in BicycleCountKind]
                elif count_type == CountKind.pedestrian:
                    count_types = [each.value for each in PedestrianCountKind]
                elif count_type == CountKind.no_data:
                    count_types = [each.value for each in NotInDatabaseCountKind]

                bind_names = ",".join(":" + str(i + 1) for i in range(len(count_types)))
                cursor.execute(
                    f"select recordnum from tc_header where type in ({bind_names}) order by recordnum desc",
                    count_types,
                )

            res = cursor.fetchall()

    records = []
    for row in res:
        records.append(row[0])

    return records


@router.get(
    "/records/{num}",
    responses=responses,
)
def get_metadata_json(num: int) -> Optional[Metadata]:
    """
    Get metadata for a count.
    """

    try:
        metadata = get_metadata(num)
    except ValidationError:
        return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})

    if metadata is None:
        return None

    return metadata


def get_metadata(num: int) -> Optional[Metadata]:
    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            # get overall count metadata
            cursor.execute("select * from tc_header where recordnum = :num", num=num)

            # convert tuple to dictionary
            # <https://python-oracledb.readthedocs.io/en/latest/user_guide/sql_execution.html#rowfactories>
            columns = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            metadata = cursor.fetchone()

            if metadata is None:
                return None
            try:
                metadata = Metadata(**metadata)
            except ValidationError:
                raise

            # map SOURCE to human-readable version
            if metadata.SOURCE == "0":
                metadata.SOURCE = "DVRPC"
            elif metadata.SOURCE == "-1":
                metadata.SOURCE = "external"

            # get MCD names
            if metadata.MCD:
                cursor.execute(
                    "select mcdname, county, state from tc_mcd where dvrpc = :mcd",
                    mcd=metadata.MCD,
                )

                columns = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                mcd_data = cursor.fetchone()

                if mcd_data:
                    metadata.municipality = mcd_data["MCDNAME"]
                    metadata.county = mcd_data["COUNTY"]
                    metadata.state = mcd_data["STATE"]

            if metadata.TYPE in [each.value for each in BicycleCountKind]:
                metadata.count_type = CountKind.bicycle
            elif metadata.TYPE in [each.value for each in PedestrianCountKind]:
                metadata.count_type = CountKind.pedestrian
            elif metadata.TYPE in [each.value for each in VehicleCountKind]:
                metadata.count_type = CountKind.vehicle
            elif metadata.TYPE in [each.value for each in NotInDatabaseCountKind]:
                metadata.count_type = CountKind.no_data

            return metadata
