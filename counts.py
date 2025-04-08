from enum import Enum


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


# "8 Day" and "Loop" are currently being recategorized into one of the others in this class
class VehicleCountKind(str, Enum):
    volume = "Volume"
    fifteen_min_volume = "15 min Volume"
    _class = "Class"
    speed = "Speed"
    eight_day = "8 Day"
    loop = "Loop"


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
