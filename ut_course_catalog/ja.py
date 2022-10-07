"""Main module."""
import math
import re
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Iterable, NamedTuple, Optional, TypeVar, Union

import aiohttp
from bs4 import BeautifulSoup, ResultSet, Tag

from ut_course_catalog.common import Semester, Weekday

BASE_URL = "https://catalog.he.u-tokyo.ac.jp/"


class Institution(Enum):
    """Institution in the University of Tokyo."""

    学部前期課程 = "jd"
    """Junior Division"""
    学部後期課程 = "ug"
    """Senior Division"""
    大学院 = "g"
    """Graduate"""
    All = "all"


class Faculty(IntEnum):
    """Faculty in the University of Tokyo."""

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

    @classmethod
    def value_of(cls, value) -> "Faculty":
        """Converts a commonly used expression in the website to a Faculty enum value."""
        for k, v in cls.__members__.items():
            if k == value:
                return v
        if value == "教養学部（前期課程）":
            return cls.教養学部前期課程
        else:
            raise ValueError(f"'{cls.__name__}' enum not found for '{value}'")


class SearchResultItem(NamedTuple):
    """Summary of a course in search results. Call `fetch_details` to get more information."""

    時間割コード: str
    共通科目コード: str
    コース名: str
    教員: str
    学期: Iterable[Semester]
    曜限: Iterable[tuple[Weekday, int]]
    ねらい: str


class SearchResult(NamedTuple):
    """Result of a search query."""

    items: Iterable[SearchResultItem]
    current_items_first_index: int
    current_items_last_index: int
    current_items_count: int
    total_items_count: int
    current_page: int
    total_pages: int


class Details(NamedTuple):
    """Details of a course. Contains all available information for a course on the website. (UTAS may have more information)"""

    時間割コード: str
    共通科目コード: str
    コース名: str
    教員: str
    学期: Iterable[Semester]
    曜限: Iterable[tuple[Weekday, int]]
    ねらい: str
    教室: str
    単位数: int
    他学部履修可: bool
    講義使用言語: str
    実務経験のある教員による授業科目: bool
    開講所属: Faculty
    授業計画: Optional[str]
    授業の方法: Optional[str]
    成績評価方法: Optional[str]
    教科書: Optional[str]
    参考書: Optional[str]
    履修上の注意: Optional[str]


T = TypeVar("T")
IterableOrType = Union[Iterable[T], T]
OptionalIterableOrType = Optional[IterableOrType[T]]


@dataclass
class SearchParams:
    """Search query parameters."""

    keyword: Optional[str] = None
    課程: Institution = Institution.All
    開講所属: Optional[Faculty] = None
    学年: OptionalIterableOrType[int] = None
    """AND search, not OR."""
    学期: OptionalIterableOrType[Semester] = None
    """AND search, not OR."""
    曜日: OptionalIterableOrType[Weekday] = None
    """AND search, not OR. Few courses have multiple periods."""
    時限: OptionalIterableOrType[int] = None
    """AND search, not OR. Few courses have multiple periods."""
    講義使用言語: OptionalIterableOrType[str] = None
    """AND search, not OR."""
    横断型教育プログラム: OptionalIterableOrType[str] = None
    """AND search, not OR."""
    実務経験のある教員による授業科目: OptionalIterableOrType[bool] = None
    """AND search, not OR. Do not specify [True, False] though it is valid."""
    分野_NDC: OptionalIterableOrType[str] = None
    """AND search, not OR."""


def _format(text: str) -> str:
    """Utility function for removing unnecessary whitespaces."""
    table = str.maketrans("　", " ", " \n\r\t")
    return text.translate(table)


def _format_description(text: str) -> str:
    # delete spaces at first and last
    text = re.sub(r"^\s+", "", text)
    text = re.sub(r"\s+$", "", text)
    # table = str.maketrans("", "", "\r\n\t")
    # text = text.translate(table)
    return text


def _ensure_found(obj: object) -> Tag:
    if type(obj) is not Tag:
        raise ParserError(f"{obj} not found")
    return obj


def _parse_weekday_period(period_text: str) -> set[tuple[Weekday, int]]:
    period_text = _format(period_text)
    if period_text == "集中":
        return set()
    period_texts = period_text.split("、")

    def parse_one(period: str):
        w = Weekday([weekday in period for weekday in list("月火水木金土日集")].index(True))
        reres = re.search(r"\d+", period)
        if not reres:
            raise ValueError(f"Invalid period: {period}")
        p = int(reres.group())
        return w, p

    result = set()
    for item in period_texts:
        result.add(parse_one(item))
    return result


class ParserError(Exception):
    pass


class UTCourseCatalog:
    """A parser for the [UTokyo Online Course Catalogue](https://catalog.he.u-tokyo.ac.jp)."""

    session: Optional[aiohttp.ClientSession]

    def __init__(self) -> None:
        self.session = None

    async def __aenter__(self):
        if self.session:
            raise RuntimeError("__aenter__ called twice")
        self.session = aiohttp.ClientSession()
        await self.session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._check_client()
        assert self.session

        await self.session.__aexit__(exc_type, exc, tb)

    def _check_client(self):
        if not self.session:
            raise RuntimeError("__aenter__ not called")

    async def fetch_search(self, params: SearchParams, page: int = 1) -> SearchResult:
        """Fetch search results from the website.

        Parameters
        ----------
        params : SearchParams
            Search parameters.
        page : int, optional
            page number, by default 1

        Returns
        -------
        SearchResult
            Search results.

        Raises
        ------
        ParserError
            Raises when failed to parse the website.
        """
        self._check_client()
        assert self.session
        # See: https://github.com/34j/ut-course-catalog-swagger/blob/master/swagger.yaml

        # build query
        _params = {
            "type": params.課程.value,
            "page": page,
        }
        if params.keyword:
            _params["q"] = params.keyword
        if params.開講所属:
            _params["faculty_id"] = params.開講所属.value

        def iterable_or_type_to_iterable(
            x: IterableOrType[T],
        ) -> Iterable[T]:
            if isinstance(x, Iterable):
                return x
            return [x]

        # build facet query
        facet = {}
        if params.横断型教育プログラム:
            facet["uwide_cross_program_codes"] = iterable_or_type_to_iterable(
                params.横断型教育プログラム
            )
        if params.学年:
            facet["grades_codes"] = iterable_or_type_to_iterable(params.学年)
        if params.学期:
            facet["semester_codes"] = [
                s.value for s in iterable_or_type_to_iterable(params.学期)
            ]
        if params.時限:
            facet["period_codes"] = [
                x - 1 for x in iterable_or_type_to_iterable(params.時限)
            ]
        if params.曜日 is not None:
            facet["wday_codes"] = [
                x.value * 100 + 1000 for x in iterable_or_type_to_iterable(params.曜日)
            ]
        if params.講義使用言語:
            facet["course_language_codes"] = iterable_or_type_to_iterable(params.講義使用言語)
        if params.実務経験のある教員による授業科目:
            facet["operational_experience_flag"] = iterable_or_type_to_iterable(
                params.実務経験のある教員による授業科目
            )
        if params.分野_NDC:
            # subject_code is not typo, it is a typo in the API
            facet["subject_code"] = iterable_or_type_to_iterable(params.分野_NDC)
        facet = {k: [str(x) for x in v] for k, v in facet.items()}
        if facet:
            _params["facet"] = str(facet).replace("'", '"').replace(" ", "")

        # fetch website
        async with self.session.get(BASE_URL + "result", params=_params) as response:
            # parse website
            soup = BeautifulSoup(await response.text(), "html.parser")

            # get page info first
            page_info_element = soup.find(class_="catalog-total-search-result")
            if not page_info_element:
                # not found
                return SearchResult(
                    items=[],
                    current_items_count=0,
                    total_items_count=0,
                    current_items_first_index=0,
                    current_items_last_index=0,
                    current_page=0,
                    total_pages=0,
                )

            page_info_text = _format(page_info_element.text)
            page_info_match: list[str] = re.findall(r"\d+", page_info_text)
            current_items_first_index = int(page_info_match[0])
            current_items_last_index = int(page_info_match[1])
            current_items_count = (
                current_items_last_index - current_items_first_index + 1
            )
            total_items_count = int(page_info_match[2])
            total_pages = math.ceil(total_items_count / 10)

            def get_items() -> Iterable[SearchResultItem]:
                """Get search result items."""
                container = soup.find(
                    "div", class_="catalog-search-result-card-container"
                )
                if container is None:
                    return
                if type(container) is not Tag:
                    raise ParserError(f"container not found: {container}")
                cards = container.find_all("div", class_="catalog-search-result-card")
                for card in cards:
                    cells_parent: Tag = card.find_all(
                        class_="catalog-search-result-table-row"
                    )[1]
                    if not cells_parent:
                        continue

                    def get_cell(name: str) -> Tag:
                        cell = cells_parent.find("div", class_=f"{name}-cell")
                        if type(cell) is not Tag:
                            raise ParserError(f"cell not found: {name}")
                        return cell

                    def get_cell_text(name: str) -> str:
                        cell = get_cell(name)
                        return _format(cell.text)

                    code_cell = _ensure_found(cells_parent.find(class_="code-cell"))
                    code_cell_children = list(code_cell.children)
                    yield SearchResultItem(
                        ねらい=_format_description(
                            card.find(
                                class_="catalog-search-result-card-body-text"
                            ).text
                        ),
                        時間割コード=code_cell_children[1].text,
                        共通科目コード=code_cell_children[3].text,
                        コース名=get_cell_text("name"),
                        教員=get_cell_text("lecturer"),
                        学期=[
                            Semester(el.text.replace(" ", "").replace("\n", ""))
                            for el in get_cell("semester").find_all(
                                class_="catalog-semester-icon"
                            )
                        ],
                        曜限=set(_parse_weekday_period(get_cell_text("period"))),
                    )

            return SearchResult(
                items=list(get_items()),
                total_items_count=total_items_count,
                current_items_first_index=current_items_first_index,
                current_items_last_index=current_items_last_index,
                current_items_count=current_items_count,
                total_pages=total_pages,
                current_page=page,
            )

    async def fetch_detail(self, code: str, year: int = 2022) -> Details:
        """Fetch details of a course.

        Parameters
        ----------
        code : str
            Course (common) code.
        year : int, optional
            Year of the course, by default 2022.

        Returns
        -------
        Details
            Details of the course.

        Raises
        ------
        ParserError
            Raises when the parser fails to parse the website.
        """
        self._check_client()
        assert self.session

        async with self.session.get(
            BASE_URL + "detail", params={"code": code, "year": str(year)}
        ) as response:
            """
            We get information from 3 different types of elements:
                cells 1: cells in the smallest table in the page.
                cells 2: cells in the first card.
                cards: cards.
            """

            # parse html
            soup = BeautifulSoup(await response.text(), "html.parser")

            # utility functions to get elements and their text
            cells1_parent: Tag = soup.find_all(class_="catalog-row")[1]

            def get_cell1(name: str) -> str:
                class_ = f"{name}-cell"
                cell = cells1_parent.find("div", class_=class_)
                if not cell:
                    raise ParserError(f"Cell {name} not found")
                return _format(cell.text)

            def get_cell2(index: int) -> str:
                class_ = f"td{index // 3 + 1}-cell"
                return _format(soup.find_all(class_=class_)[index % 3].text)

            def get_cards():
                cards: ResultSet[Tag] = soup.find_all(class_="catalog-page-detail-card")
                for card in cards:
                    card_header = card.find(class_="catalog-page-detail-card-header")
                    if not card_header:
                        raise ParserError("Card header not found")
                    title = _format(card_header.text)
                    card_body = card.find(class_="catalog-page-detail-card-body-pre")
                    if not card_body:
                        raise ParserError("card_body not found")
                    if type(card_body) is not Tag:
                        raise ParserError("card_body is not Tag")
                    yield title, card_body

            cards = dict(get_cards())

            def get_card(name: str) -> Optional[Tag]:
                return cards.get(name, None)

            def get_card_text(name: str) -> Optional[str]:
                if card := get_card(name):
                    return _format_description(card.text)
                return None

            code_cell = _ensure_found(cells1_parent.find(class_="code-cell"))
            code_cell_children = list(code_cell.children)

            # return the result
            return Details(
                時間割コード=code_cell_children[1].text,
                共通科目コード=code_cell_children[3].text,
                コース名=get_cell1("name"),
                教員=get_cell1("lecturer"),
                学期=[
                    Semester(el.text.replace(" ", "").replace("\n", ""))
                    for el in cells1_parent.find_all(class_="catalog-semester-icon")
                ],
                曜限=_parse_weekday_period(get_cell1("period")),
                教室=get_cell2(0),
                単位数=int(get_cell2(1)),
                他学部履修可="不可" not in get_cell2(2),
                講義使用言語=get_cell2(3),
                実務経験のある教員による授業科目="YES" in get_cell2(4),
                開講所属=Faculty.value_of(get_cell2(5)),
                授業計画=get_card_text("授業計画"),
                授業の方法=get_card_text("授業の方法"),
                成績評価方法=get_card_text("成績評価方法"),
                教科書=get_card_text("教科書"),
                参考書=get_card_text("参考書"),
                履修上の注意=get_card_text("履修上の注意"),
                ねらい=_format(
                    _ensure_found(
                        soup.find(class_="catalog-page-detail-lecture-aim")
                    ).text
                ),
            )
