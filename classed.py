import csv
import datetime
import logging
import os
from pathlib import Path
from typing import Any, Optional

import oracledb
from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ValidationError, AliasGenerator

from common import NotFoundError, responses
from config import PASSWORD, USER
from metadata import Metadata, get_metadata

router = APIRouter()

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler("../class.log"))
logger.propagate = False


class HourlyClass(BaseModel):
    """
    Hourly volume by class.
    NOTE: Unclassified vehicles are listed in their own field, but are also included in the count
    for passenger cars.
    """

    DATETIME: datetime.datetime
    TOTAL: int
    MOTORCYCLES: int
    PASSENGER_CARS: int
    OTHER_FOUR_TIRE_SINGLE_UNIT_VEHICLES: int
    BUSES: int
    TWO_AXLE_SIX_TIRE_SINGLE_UNIT_TRUCKS: int
    THREE_AXLE_SINGLE_UNIT_TRUCKS: int
    FOUR_OR_MORE_AXLE_SINGLE_UNIT_TRUCKS: int
    FOUR_OR_FEWER_AXLE_SINGLE_TRAILER_TRUCKS: int
    FIVE_AXLE_SINGLE_TRAILER_TRUCKS: int
    SIX_OR_MORE_AXLE_SINGLE_TRAILER_TRUCKS: int
    FIVE_OR_FEWER_AXLE_MULTI_TRAILER_TRUCKS: int
    SIX_AXLE_MULTI_TRAILER_TRUCKS: int
    SEVEN_OR_MORE_AXLE_MULTI_TRAILER_TRUCKS: int
    UNCLASSIFIED_VEHICLE: Optional[int]

    class Config:
        validate_by_name = True
        alias_generator = AliasGenerator(
            serialization_alias=lambda field_name: field_name.lower(),
        )


class HourlyClassRecord(BaseModel):
    """
    A class count by hour.
    """

    metadata: Metadata
    static_pdf: Optional[str]
    counts: list[HourlyClass]


def get_hourly_class(num: int) -> Optional[HourlyClassRecord]:
    try:
        metadata = get_metadata(num)
    except ValidationError:
        raise

    if metadata is None:
        return None

    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select 
                    TRUNC(countdatetime, 'HH24') as datetime,
                    sum(total) as total,
                    sum(bikes) as motorcycles,
                    sum(cars_and_tlrs) as PASSENGER_CARS,
                    sum(ax2_long) as OTHER_FOUR_TIRE_SINGLE_UNIT_VEHICLES,
                    sum(buses) as buses,
                    sum(ax2_6_tire) as TWO_AXLE_SIX_TIRE_SINGLE_UNIT_TRUCKS,
                    sum(ax3_single) as THREE_AXLE_SINGLE_UNIT_TRUCKS,
                    sum(ax4_single) as FOUR_OR_MORE_AXLE_SINGLE_UNIT_TRUCKS,
                    sum(lt_5_ax_double) as FOUR_OR_FEWER_AXLE_SINGLE_TRAILER_TRUCKS,
                    sum(ax5_double) as FIVE_AXLE_SINGLE_TRAILER_TRUCKS,
                    sum(gt_5_ax_double) as SIX_OR_MORE_AXLE_SINGLE_TRAILER_TRUCKS,
                    sum(lt_6_ax_multi) as FIVE_OR_FEWER_AXLE_MULTI_TRAILER_TRUCKS,
                    sum(ax6_multi) as SIX_AXLE_MULTI_TRAILER_TRUCKS,
                    sum(gt_6_ax_multi) as SEVEN_OR_MORE_AXLE_MULTI_TRAILER_TRUCKS,
                    sum(unclassified) as UNCLASSIFIED_VEHICLE
                from tc_clacount_new 
                where recordnum = :num 
                group by trunc(countdatetime, 'HH24')
                order by trunc(countdatetime, 'HH24')
                """,
                num=num,
            )

            columns = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            count_data = cursor.fetchall()

            hourly_class = HourlyClassRecord(metadata=metadata, static_pdf=None, counts=count_data)

    return hourly_class


@router.get(
    "/class/hourly/{num}",
    responses=responses,
    response_model=HourlyClassRecord,
    summary="Get hourly volume by class of a count in JSON format",
)
def get_hourly_class_json(num: int) -> Any:
    try:
        record = get_hourly_class(num)
    except ValidationError:
        return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})

    if record is None:
        return JSONResponse(status_code=404, content={"message": "Record not found"})

    return record


@router.get(
    "/class/hourly/csv/{num}",
    responses=responses,  # type: ignore
    response_model=HourlyClassRecord,
    summary="Get hourly volume by class of a count in CSV file",
)
def get_hourly_class_csv(num: int) -> Any:
    """
    Metadata will be placed in the first two rows, followed by a blank line, followed by the
    data from the count.
    """
    # Create csv/ folder if it doesn't exist.
    try:
        Path("csv/class").mkdir()
    except FileExistsError:
        pass
    csv_file = Path(f"csv/class/{num}.csv")

    if csv_file.exists():
        # If older than most recent AADV calculation, we have to recreate it.
        try:
            csv_seconds_from_epoch = os.path.getmtime(csv_file)
            csv_created_date = datetime.date.fromtimestamp(csv_seconds_from_epoch)

            with oracledb.connect(
                user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls"
            ) as connection:
                with connection.cursor() as cursor:
                    # get overall count metadata
                    cursor.execute(
                        "select date_calculated from aadv where recordnum = :num order by date_calculated desc",
                        num=num,
                    )
                    aadv_created_date = cursor.fetchone()[0]
                    aadv_created_date = aadv_created_date.date()

            if csv_created_date < aadv_created_date:
                try:
                    create_hourly_class_csv(csv_file, num)
                except NotFoundError:
                    return JSONResponse(status_code=404, content={"message": "Record not found"})
                except ValidationError:
                    return JSONResponse(
                        status_code=500, content={"message": "Unexpected data type found."}
                    )
        # If any exception occurred above that wasn't already handled, just create the CSV file.
        except Exception:
            try:
                create_hourly_class_csv(csv_file, num)
            except NotFoundError:
                return JSONResponse(status_code=404, content={"message": "Record not found"})
            except ValidationError:
                return JSONResponse(
                    status_code=500, content={"message": "Unexpected data type found."}
                )
            except Exception:
                return JSONResponse(
                    status_code=500, content={"message": "Unhandled error occurred."}
                )
    # No CSV has been created yet, so create one.
    else:
        try:
            create_hourly_class_csv(csv_file, num)
        except NotFoundError:
            return JSONResponse(status_code=404, content={"message": "Record not found"})
        except ValidationError:
            return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})
        except Exception:
            return JSONResponse(status_code=500, content={"message": "Unhandled error occurred."})

    return FileResponse(csv_file)


def create_hourly_class_csv(csv_path: Path, num: int):
    """
    Create a CSV file of hourly volume by class of a count.
    """
    record = get_hourly_class(num)

    if record is None:
        raise NotFoundError

    # Create CSV, save it, return it.
    with open(csv_path, "w", newline="") as f:
        # Get and write metadata field names and values.
        fieldnames_metadata = list(Metadata.model_json_schema()["properties"].keys())
        fieldnames_metadata = [field.lower() for field in fieldnames_metadata]
        writer = csv.DictWriter(f, fieldnames=fieldnames_metadata, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(record.metadata.model_dump(by_alias=True))

        # Create new writer, just to write an empty line to the same file.
        writer = csv.writer(f)
        writer.writerow("")

        # Get and write count field names and values; use new writer to add it to the same file.
        fieldnames_count = list(HourlyClass.schema()["properties"].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames_count)
        writer.writeheader()
        for count in record.counts:
            writer.writerow(count.dict(by_alias=True))

    return
