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

from common import NotFoundError, responses
from config import PASSWORD, USER
from counts import (
    BicycleCountKind,
    NotInDatabaseCountKind,
    PedestrianCountKind,
    VehicleCountKind,
)
from metadata import Metadata, get_metadata

router = APIRouter()

logger2 = logging.getLogger(__name__)
logger2.addHandler(logging.FileHandler("../api.log"))
logger2.propagate = False


class NonNormalHourlyCount(BaseModel):
    date: datetime.date = None
    AM12: Optional[int] = None
    AM1: Optional[int] = None
    AM2: Optional[int] = None
    AM3: Optional[int] = None
    AM4: Optional[int] = None
    AM5: Optional[int] = None
    AM6: Optional[int] = None
    AM7: Optional[int] = None
    AM8: Optional[int] = None
    AM9: Optional[int] = None
    AM10: Optional[int] = None
    AM11: Optional[int] = None
    PM12: Optional[int] = None
    PM1: Optional[int] = None
    PM2: Optional[int] = None
    PM3: Optional[int] = None
    PM4: Optional[int] = None
    PM5: Optional[int] = None
    PM6: Optional[int] = None
    PM7: Optional[int] = None
    PM8: Optional[int] = None
    PM9: Optional[int] = None
    PM10: Optional[int] = None
    PM11: Optional[int] = None
    total: Optional[int] = None


class NonNormalHourlyVolumeRecord(BaseModel):
    """
    A non-normal hourly volume count.
    """

    metadata: Metadata
    static_pdf: Optional[str]
    counts: list[NonNormalHourlyCount]


def get_hourly_volume(num: int) -> Optional[NonNormalHourlyVolumeRecord]:
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

    try:
        metadata = get_metadata(num)
    except ValidationError:
        raise

    if metadata is None:
        return None

    with oracledb.connect(user=USER, password=PASSWORD, dsn="dvrpcprod_tp_tls") as connection:
        with connection.cursor() as cursor:
            if metadata.TYPE in (
                [each.value for each in BicycleCountKind]
                + [each.value for each in PedestrianCountKind]
                + [each.value for each in VehicleCountKind]
            ):
                if metadata.TYPE in [each.value for each in BicycleCountKind]:
                    tc_table = "tc_bikecount_new"

                if metadata.TYPE in [each.value for each in PedestrianCountKind]:
                    tc_table = "tc_pedcount_new"

                if metadata.TYPE in [each.value for each in VehicleCountKind]:
                    tc_table = "tc_volcount_new"

                cursor.execute(
                    f"""
                    select
                        to_char(countdatetime, 'YYYY-MM-DD') as count_date,
                        to_char(countdatetime, 'HH24') as hour,
                        sum(volume) as hourly_volume
                    from {tc_table}
                    where recordnum = :num
                    group by to_char(countdatetime, 'YYYY-MM-DD'), to_char(countdatetime, 'HH24')
                    order by to_char(countdatetime, 'YYYY-MM-DD'), to_char(countdatetime, 'HH24')
                """,
                    # order by to_char(countdatetime, 'YYYY-MM-DD'), to_char( countdatetime, 'hh24')
                    num=num,
                )

                columns = [col[0] for col in cursor.description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                count_data = cursor.fetchall()

                # that returns a list of dicts in the form
                # {countdate, hour, total}
                # create an intermediate dict of dicts to combine all hours/total by date
                # {date: { am1, am2, ... pm12 ... pm11, total}}

                counts = {}

                if count_data:
                    for row in count_data:
                        # create new entry if it doesn't yet exist
                        if not counts.get(row["COUNT_DATE"]):
                            counts[row["COUNT_DATE"]] = {}

                        # populate the hourly volume
                        for k, v in am_pm_map.items():
                            if row["HOUR"] == k:
                                counts[row["COUNT_DATE"]][v] = row["HOURLY_VOLUME"]

                    # sum total by day
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

                        columns = [col[0] for col in cursor.description]
                        cursor.rowfactory = lambda *args: dict(zip(columns, args))

                # change this dict of dicts into list of Counts and add to record
                counts = [NonNormalHourlyCount(date=k, **v) for k, v in counts.items()]
                hourly_volume = NonNormalHourlyVolumeRecord(
                    metadata=metadata, static_pdf=None, counts=counts
                )

            # these are not in the database but just in static pdf
            elif metadata.TYPE in [each.value for each in NotInDatabaseCountKind]:
                # the subtype in the url is just the value of TYPE without spaces
                sub_type_in_url = metadata.TYPE.value.replace(" ", "")
                static_pdf = f"https://www.dvrpc.org/asp/TrafficCountPDF/{sub_type_in_url}/{metadata.RECORDNUM}.PDF"
                hourly_volume = NonNormalHourlyVolumeRecord(
                    metadata=metadata, static_pdf=static_pdf, counts=[]
                )

    return hourly_volume


@router.get(
    "/volume/hourly/non-normal/{num}",
    responses=responses,
    response_model=NonNormalHourlyVolumeRecord,
    summary="Get non-normal count data (by hours in a day) in JSON format",
)
def get_hourly_volume_json(num: int) -> Any:
    try:
        record = get_hourly_volume(num)
    except ValidationError:
        return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})

    if record is None:
        return JSONResponse(status_code=404, content={"message": "Record not found"})

    return record


@router.get(
    "/volume/hourly/non-normal/csv/{num}",
    responses=responses,  # type: ignore
    response_model=NonNormalHourlyVolumeRecord,
    summary="Get non-normal count data (by hours in a day) in a CSV file",
)
def get_hourly_volume_csv(num: int) -> Any:
    """
    Metadata will be placed in the first two rows, followed by a blank line, followed by the
    data from the count.
    """
    # Create csv/ folder if it doesn't exist.
    try:
        Path("csv").mkdir()
    except FileExistsError:
        pass
    csv_file = Path(f"csv/{num}.csv")

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
                    create_hourly_nonnormal_csv(csv_file, num)
                except NotFoundError:
                    return JSONResponse(status_code=404, content={"message": "Record not found"})
                except ValidationError:
                    return JSONResponse(
                        status_code=500, content={"message": "Unexpected data type found."}
                    )
        # If any exception occurred above that wasn't already handled, just create the CSV file.
        except Exception:
            try:
                create_hourly_nonnormal_csv(csv_file, num)
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
            create_hourly_nonnormal_csv(csv_file, num)
        except NotFoundError:
            return JSONResponse(status_code=404, content={"message": "Record not found"})
        except ValidationError:
            return JSONResponse(status_code=500, content={"message": "Unexpected data type found."})
        except Exception:
            return JSONResponse(status_code=500, content={"message": "Unhandled error occurred."})

    return FileResponse(csv_file)


def create_hourly_nonnormal_csv(csv_path: Path, num: int):
    """
    Create a CSV file of non-normal hourly data.
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

        # Create new writer, just to write an empty line to the same file.
        writer = csv.writer(f)
        writer.writerow("")

        # Get and write count field names and values; use new writer to add it to the same file.
        fieldnames_count = list(NonNormalHourlyCount.schema()["properties"].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames_count)
        writer.writeheader()
        for count in record.counts:
            writer.writerow(count.dict(by_alias=True))

    return
