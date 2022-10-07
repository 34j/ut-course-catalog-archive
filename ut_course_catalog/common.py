from enum import Enum, IntEnum


class Semester(Enum):
    A1 = "A1"
    A2 = "A2"
    S1 = "S1"
    S2 = "S2"
    W = "W"


class Weekday(IntEnum):
    Mon = 0
    Tue = 1
    Wed = 2
    Thu = 3
    Fri = 4
    Sat = 5
    Sun = 6
