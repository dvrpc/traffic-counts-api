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

from common import NotFoundError, NotPublishedError, responses, get_suppressed_dates
from config import PASSWORD, USER
from counts import (
    BicycleCountKind,
    NotInDatabaseCountKind,
    PedestrianCountKind,
    VehicleCountKind,
)
from metadata import Metadata, get_metadata

router = APIRouter()

logger = logging.getLogger("api")


class NonNormalHourlyCount(BaseModel):
    date: datetime.date
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
    suppressed_dates: list[datetime.date]
    counts: list[NonNormalHourlyCount]


def get_hourly_volume(num: int, include_suppressed: bool) -> NonNormalHourlyVolumeRecord:
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

                suppressed_dates = get_suppressed_dates(cursor, num)

                if not include_suppressed:
                    counts = [count for count in counts if count.date not in suppressed_dates]

                hourly_volume = NonNormalHourlyVolumeRecord(
                    metadata=metadata,
                    static_pdf=None,
                    suppressed_dates=suppressed_dates,
                    counts=counts,
                )

            # these are not in the database but just in static pdf
            elif metadata.sub_type in [each for each in NotInDatabaseCountKind]:
                # the subtype in the url is just the value of TYPE without spaces
                sub_type_in_url = metadata.sub_type.replace(" ", "")
                static_pdf = f"https://idnryib36jqh.objectstorage.us-ashburn-1.oci.customer-oci.com/p/CRSJ63CubnhBU9YppzcOZJuiALo2cVZsKVCBEEc_zt2UkPrQJaVId3Q5G2iQfiMB/n/idnryib36jqh/b/web-static-content/o/TrafficCountPDF/{sub_type_in_url}/{metadata.RECORDNUM}.pdf"
                hourly_volume = NonNormalHourlyVolumeRecord(
                    metadata=metadata, static_pdf=static_pdf, suppressed_dates=[], counts=[]
                )

    return hourly_volume


@router.get(
    "/volume/hourly/non-normal/{num}",
    responses=responses,
    response_model=NonNormalHourlyVolumeRecord,
    summary="Get non-normal count data (by hours in a day) in JSON format",
)
def get_hourly_volume_json(num: int, include_suppressed: bool = False) -> Any:
    try:
        record = get_hourly_volume(num, include_suppressed)
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
    "/volume/hourly/non-normal/csv/{num}",
    responses=responses,  # type: ignore
    response_model=NonNormalHourlyVolumeRecord,
    summary="Get non-normal count data (by hours in a day) as a CSV file",
)
def get_hourly_volume_csv(num: int, include_suppressed: bool = False) -> Any:
    """
    Metadata will be placed in the first two rows, followed by a blank line, followed by the
    data from the count.
    """
    # Create csv/ folder if it doesn't exist.
    try:
        Path("csv/non_normal_volume").mkdir()
    except FileExistsError:
        pass
    csv_file = Path(f"csv/non_normal_volume/{num}.csv")

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
                    create_hourly_nonnormal_csv(csv_file, num, include_suppressed)
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
                create_hourly_nonnormal_csv(csv_file, num, include_suppressed)
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
            create_hourly_nonnormal_csv(csv_file, num, include_suppressed)
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


def create_hourly_nonnormal_csv(csv_path: Path, num: int, include_suppressed: bool):
    """
    Create a CSV file of non-normal hourly data.
    """
    record = get_hourly_volume(num, include_suppressed)

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

        # Write counts.
        writer.writerow("")
        writer.writerow(record.counts[0].model_dump(by_alias=True))
        for count in record.counts:
            writer.writerow([v for k, v in count])

    return
