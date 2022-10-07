from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import NamedTuple, Optional
from typing import Iterable
from .common import Semester, Weekday


class Institution(Enum):
    JuniorDivision = "jd"
    SeniorDivision = "ug"
    Graduate = "g"
    All = "all"


class Faculty(IntEnum):
    法学部 = 1
    医学部 = 2
    工学部 = 3
    文学部 = 4
    理学部 = 5
    農学部 = 6
    経済学部 = 7
    教養学部 = 8
    教育学部 = 9
    薬学部 = 10
    人文社会系研究科 = 11
    教育学研究科 = 12
    法学政治学研究科 = 13
    経済学研究科 = 14
    総合文化研究科 = 15
    理学系研究科 = 16
    工学系研究科 = 17
    農学生命科学研究科 = 18
    医学系研究科 = 19
    薬学系研究科 = 20
    数理科学研究科 = 21
    新領域創成科学研究科 = 22
    情報理工学研究科 = 23
    学際情報学府 = 24
    公共政策学教育部 = 25
    教養学部前期課程 = 26


class Details(NamedTuple):
    common_course_code: str
    catalog_code: str
    title: str
    lecturer: str
    semester: Iterable[Semester]
    weekday: Weekday
    period: int
    room: str
    credit: int
    other_faculty: bool
    language: str
    practical_experience: bool
    faculty: Faculty
    schedule: str
    teaching_methods: str
    method_of_evaluation: str
    required_textbook: str
    reference_books: str
    notes_on_taking_the_course: str
    others: str


@dataclass
class SearchParams:
    name: Optional[str] = None
    type: Institution = Institution.All
    faculty: Optional[Faculty] = None
    grades: Optional[Iterable[int]] = None
    semesters: Optional[Iterable[Semester]] = None
    weekdays: Optional[Iterable[Weekday]] = None
    course_languages: Optional[Iterable[str]] = None
    uwide_course_programs: Optional[Iterable[str]] = None
    periods: Optional[Iterable[int]] = None
    practical_experience_flags: Optional[Iterable[bool]] = None
    ndc_codes: Optional[Iterable[int]] = None
