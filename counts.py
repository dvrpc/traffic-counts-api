from enum import Enum
from itertools import chain

# The kinds of counts in the tc_counttype table are grouped below into various
# CountKinds according to their structure (or excluded altogether if not in database, which
# is the NotInDatabaseCountKind), and then grouped into the categories we ulimately want -
# vehicle, bicycle, or pedestrian.


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


class VehicleCountKind(str, Enum):
    volume = "Volume"
    fifteen_min_volume = "15 min Volume"
    _class = "Class"
    speed = "Speed"


# Individual counts not in database.
class NotInDatabaseCountKind(str, Enum):
    turning_movement = "Turning Movement"
    manual_class = "Manual Class"
    crosswalk = "Crosswalk"


# The broad grouping of count types.
class CountKind(str, Enum):
    vehicle = "vehicle"
    bicycle = "bicycle"
    pedestrian = "pedestrian"
    no_data = "count data not in database"


class AllCountKinds(Enum):
    """All kinds of counts."""

    # This is for use in metadata.py's /record endpoint. Using `Union` is the obvious way to do
    # this, but FastAPI then only includes the first enum from that Union in the OpenAPI docs.
    # From <https://stackoverflow.com/q/71527981>

    _ignore_ = "member cls"
    cls = vars()
    for member in chain(list(BicycleCountKind), list(PedestrianCountKind)):
        cls[member.name] = member.value

    def __str__(self):
        return str(self.value)
