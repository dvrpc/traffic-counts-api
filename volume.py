import csv
import datetime
import logging
import os
from pathlib import Path
from typing import Any, Optional

import oracledb
from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ValidationError

from common import NotFoundError, NotPublishedError, responses
from config import PASSWORD, USER
from counts import (
    BicycleCountKind,
    CountKind,
    NotInDatabaseCountKind,
    PedestrianCountKind,
    VehicleCountKind,
)
from metadata import Metadata, get_metadata

router = APIRouter()

logger = logging.getLogger("api")


class HourlyVolumeRecord(BaseModel):
    """
    An volume count by hour.
    """

    metadata: Metadata
    static_pdf: Optional[str]
    counts: dict[datetime.datetime, int]


def get_hourly_volume(num: int) -> HourlyVolumeRecord:
    metadata = get_metadata(num)

    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            if metadata.sub_type in (
                [each.value for each in BicycleCountKind]
                + [each.value for each in PedestrianCountKind]
                + [each.value for each in VehicleCountKind]
            ):
                if metadata.sub_type in [each.value for each in BicycleCountKind]:
                    tc_table = "tc_bikecount_new"

                if metadata.sub_type in [each.value for each in PedestrianCountKind]:
                    tc_table = "tc_pedcount_new"

                if metadata.sub_type in [each.value for each in VehicleCountKind]:
                    tc_table = "tc_volcount_new"

                cursor.execute(
                    f"""
                    select TRUNC(countdatetime, 'HH24') as datetime, sum(volume) as volume
                    from {tc_table} 
                    where recordnum = :num 
                    group by trunc(countdatetime, 'HH24')
                    order by trunc(countdatetime, 'HH24')
                    """,
                    num=num,
                )

                count_data = cursor.fetchall()
                count_data = dict((x[0], x[1]) for x in count_data)

                hourly_volume = HourlyVolumeRecord(
                    metadata=metadata, static_pdf=None, counts=count_data
                )

            # These are not in the database but just in static pdf.
            elif metadata.sub_type in [each.value for each in NotInDatabaseCountKind]:
                metadata.count_type = CountKind.no_data
                # the subtype in the url is just the value of sub_type without spaces
                sub_type_in_url = metadata.sub_type.value.replace(" ", "")
                static_pdf = f"https://www.dvrpc.org/asp/TrafficCountPDF/{sub_type_in_url}/{metadata.RECORDNUM}.PDF"
                hourly_volume = HourlyVolumeRecord(
                    metadata=metadata, static_pdf=static_pdf, counts=[]
                )

    return hourly_volume


@router.get(
    "/volume/hourly/{num}",
    responses=responses,
    response_model=HourlyVolumeRecord,
    summary="Get hourly volume of a count in JSON format",
)
def get_hourly_volume_json(num: int) -> Any:
    try:
        record = get_hourly_volume(num)
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
    "/volume/hourly/csv/{num}",
    responses=responses,  # type: ignore
    response_model=HourlyVolumeRecord,
    summary="Get hourly volume of a count as a CSV file",
)
def get_hourly_volume_csv(num: int) -> Any:
    """
    Metadata will be placed in the first two rows, followed by a blank line, followed by the
    data from the count.
    """
    # Create csv/ folder if it doesn't exist.
    try:
        Path("csv/volume").mkdir()
    except FileExistsError:
        pass
    csv_file = Path(f"csv/volume/{num}.csv")

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
                    create_hourly_csv(csv_file, num)
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
                create_hourly_csv(csv_file, num)
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
            create_hourly_csv(csv_file, num)
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


def create_hourly_csv(csv_path: Path, num: int):
    """
    Create a CSV file of hourly volume data.
    """
    record = get_hourly_volume(num)

    if record is None:
        raise NotFoundError

    # create CSV, save it, return it
    with open(csv_path, "w", newline="") as f:
        # Get and write metadata field names and values.
        fieldnames_metadata = list(Metadata.model_json_schema()["properties"].keys())
        fieldnames_metadata = [field.lower() for field in fieldnames_metadata]
        writer = csv.DictWriter(f, fieldnames=fieldnames_metadata, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(record.metadata.model_dump(by_alias=True))

        # Write counts.
        writer = csv.writer(f)
        writer.writerow("")
        writer.writerow(["datetime", "volume"])

        for k, v in record.counts.items():
            writer.writerow([k, v])

    return
