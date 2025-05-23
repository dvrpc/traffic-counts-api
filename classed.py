import csv
import datetime
import logging
import os
from pathlib import Path
from typing import Any, Optional

import oracledb
from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse
from pydantic import AliasGenerator, BaseModel, ValidationError

from common import NotFoundError, NotPublishedError, responses, get_suppressed_dates
from config import PASSWORD, USER
from metadata import Metadata, get_metadata

router = APIRouter()

logger = logging.getLogger("api")


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
    suppressed_dates: list[datetime.date]
    counts: list[HourlyClass]


def get_hourly_class(num: int, include_suppressed: bool) -> HourlyClassRecord:
    metadata = get_metadata(num)

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

            suppressed_dates = get_suppressed_dates(cursor, num)

            if not include_suppressed:
                count_data = [
                    count
                    for count in count_data
                    if count["DATETIME"].date() not in suppressed_dates
                ]

            hourly_class = HourlyClassRecord(
                metadata=metadata,
                static_pdf=None,
                suppressed_dates=suppressed_dates,
                counts=count_data,
            )

    return hourly_class


@router.get(
    "/class/hourly/{num}",
    responses=responses,
    response_model=HourlyClassRecord,
    summary="Get hourly volume by class of a count in JSON format",
)
def get_hourly_class_json(num: int, include_suppressed: bool = False) -> Any:
    try:
        record = get_hourly_class(num, include_suppressed)
    except NotFoundError as e:
        return e.json
    except NotPublishedError as e:
        return e.json
    except ValidationError as e:
        logger.error(e)
        return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})
    except Exception as e:
        logger.error(e)
        return JSONResponse(status_code=500, content={"message": "Unknown error occurred."})

    return record


@router.get(
    "/class/hourly/csv/{num}",
    responses=responses,  # type: ignore
    response_model=HourlyClassRecord,
    summary="Get hourly volume by class of a count as a CSV file",
)
def get_hourly_class_csv(num: int, include_suppressed: bool = False) -> Any:
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
                    create_hourly_class_csv(csv_file, num, include_suppressed)
                except NotFoundError as e:
                    return e.json
                except NotPublishedError as e:
                    return e.json
                except ValidationError as e:
                    logger.error(e)
                    return JSONResponse(
                        status_code=500, content={"message": "Unexpected data type found."}
                    )
                except Exception as e:
                    logger.error(e)
                    return JSONResponse(
                        status_code=500, content={"message": "Unknown error occurred."}
                    )
        # If any exception occurred above that wasn't already handled, just create the CSV file.
        except Exception:
            try:
                create_hourly_class_csv(csv_file, num, include_suppressed)
            except NotFoundError as e:
                return e.json
            except NotPublishedError as e:
                return e.json
            except ValidationError as e:
                logger.error(e)
                return JSONResponse(
                    status_code=500, content={"message": "Unexpected data type found."}
                )
            except Exception as e:
                logger.error(e)
                return JSONResponse(status_code=500, content={"message": "Unknown error occurred."})
    # No CSV has been created yet, so create one.
    else:
        try:
            create_hourly_class_csv(csv_file, num, include_suppressed)
        except NotFoundError as e:
            return e.json
        except NotPublishedError as e:
            return e.json
        except ValidationError as e:
            logger.error(e)
            return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})
        except Exception as e:
            logger.error(e)
            return JSONResponse(status_code=500, content={"message": "Unknown error occurred."})

    return FileResponse(csv_file)


def create_hourly_class_csv(csv_path: Path, num: int, include_suppressed: bool):
    """
    Create a CSV file of hourly volume by class of a count.
    """
    record = get_hourly_class(num, include_suppressed)

    if record is None:
        raise NotFoundError

    # Create and save CSV.
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)

        # Write metadata field names and values.
        writer.writerow(record.metadata.model_dump(by_alias=True))
        writer.writerow([v for k, v in record.metadata])

        # Write suppressed dates.
        writer.writerow("")
        writer.writerow(["suppressed_dates:"])
        for date in record.suppressed_dates:
            writer.writerow([date])

        # Get and write count field names and values; use new writer to add it to the same file.
        writer.writerow("")
        writer.writerow(record.counts[0].model_dump(by_alias=True))
        for count in record.counts:
            writer.writerow([v for k, v in count])

        # Add note about unclassified vehicles.
        writer.writerow("")
        note = ["" for _ in range(15)]
        note.append("Note: Unclassified vehicles are included in the 'passenger_cars' count.")
        writer.writerow(note)

    return
