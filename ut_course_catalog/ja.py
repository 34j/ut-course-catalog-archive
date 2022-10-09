from __future__ import annotations

import asyncio
import hashlib
import math
import pickle
import re
from asyncio import create_task
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from inspect import isawaitable
from logging import Logger, getLogger
from pathlib import Path
from typing import (
    AsyncIterable,
    Awaitable,
    Callable,
    Iterable,
    NamedTuple,
    Optional,
    TypeVar,
    Union,
)

import aiofiles
import aiohttp
from bs4 import BeautifulSoup, ResultSet, Tag
from pandas import DataFrame
from tenacity import WrappedFn, retry
from tenacity.before_sleep import before_sleep_log
from tenacity.stop import stop_after_attempt, stop_after_delay
from tenacity.wait import wait_exponential
from tqdm import tqdm

from ut_course_catalog.common import BASE_URL, Semester, Weekday


class Institution(Enum):
    """Institution in the University of Tokyo."""

    学部前期課程 = "jd"
    """Junior Division"""
    学部後期課程 = "ug"
    """Senior Division"""
    大学院 = "g"
    """Graduate"""
    All = "all"


class Faculty(Enum):
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
    情報理工学系研究科 = 23
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


class ClassForm(Enum):
    """
    授業形態コード 	種別
    L 	講義
    S 	演習
    E 	実験
    P	実習/実技
    T	卒業論文/卒業研究/卒業制作/論文指導/研究指導
    Z	その他"""

    講義 = "L"
    演習 = "S"
    実験 = "E"
    実習 = "P"
    卒業論文 = "T"
    その他 = "Z"


class Language(Enum):
    """Language of a course."""

    Japanese = "ja"
    English = "en"
    JapaneseAndEnglish = "ja,en"
    OtherLanguagesToo = "other"
    OnlyOtherLanguages = "only_other"
    Others = "others"


class CommonCode(str):
    @property
    def institution(self) -> Institution:
        return {"C": Institution.学部前期課程, "F": Institution.学部後期課程, "G": Institution.大学院}[
            self[0]
        ]

    @property
    def faculty(self) -> Faculty:
        """
        学部名	学部名（英語）	開講学部コード
        法学部 	Faculty of Law	LA
        医学部	Faculty of Medicine	ME
        工学部	Faculty of Engineering	EN
        文学部	Faculty of Letters 	LE
        理学部	Faculty of Science 	SC
        農学部	Faculty of Agriculture	AG
        経済学部	Faculty of Economics	EC
        教養学部	College of Arts and Sciences	AS
        教育学部	Faculty of Education	ED
        薬学部	Faculty of Pharmaceutical Sciences	PH
        人文社会系研究科	Graduate School of Humanities and Sociology	HS
        教育学研究科	Graduate School of Education	ED
        法学政治学研究科	Graduate Schools for Law and Politics	LP
        経済学研究科	Graduate School of Economics	EC
        総合文化研究科	Graduate School of Arts and Sciences	AS
        理学系研究科	Graduate School of Science	SC
        工学系研究科	Graduate School of Engineering	EN
        農学生命科学研究科	Graduate School of Agricultural and Life Sciences	AG
        医学系研究科	Graduate School of Medicine	ME
        薬学系研究科	Graduate School of Pharmaceutical Sciences	PH
        数理科学研究科	Graduate School of Mathematical Sciences	MA
        新領域創成科学研究科	Graduate School of Frontier Sciences	FS
        情報理工学系研究科	Graduate School of Information Science and Technology	IF
        学際情報学府	Graduate School of Interdisciplinary Information Studies	II
        公共政策大学院(公共政策学連携研究部・教育部)	Graduate School of Public Policy	PP
        """
        code = self[1:3]
        g_faculties = {
            "HS": Faculty.人文社会系研究科,
            "LP": Faculty.法学政治学研究科,
            "AS": Faculty.総合文化研究科,
            "SC": Faculty.理学系研究科,
            "EN": Faculty.工学系研究科,
            "AG": Faculty.農学生命科学研究科,
            "ME": Faculty.医学系研究科,
            "PH": Faculty.薬学系研究科,
            "MA": Faculty.数理科学研究科,
            "FS": Faculty.新領域創成科学研究科,
            "IF": Faculty.情報理工学系研究科,
            "II": Faculty.学際情報学府,
            "PP": Faculty.公共政策学教育部,
        }
        ug_faculties = {
            "LA": Faculty.法学部,
            "ME": Faculty.医学部,
            "EN": Faculty.工学部,
            "LE": Faculty.文学部,
            "SC": Faculty.理学部,
            "AG": Faculty.農学部,
            "EC": Faculty.経済学部,
            "AS": Faculty.教養学部,
            "ED": Faculty.教育学部,
            "PH": Faculty.薬学部,
        }
        if self.institution == Institution.大学院:
            if code in g_faculties:
                return g_faculties[code]
            if code in ug_faculties:
                return ug_faculties[code]
        else:
            if code in ug_faculties:
                return ug_faculties[code]
            if code in g_faculties:
                return g_faculties[code]
        raise ParserError(f"Unknown faculty code: {code}")

    @property
    def department(self) -> str:
        return self[4:6]

    @property
    def level(self) -> int:
        return int(self[6])

    @property
    def reference_number(self) -> int:
        return int(self[7:10])

    @property
    def class_form(self) -> ClassForm:
        """
        授業形態コード 	種別
        L 	講義
        S 	演習
        E 	実験
        P	実習/実技
        T	卒業論文/卒業研究/卒業制作/論文指導/研究指導
        Z	その他"""

        return {
            "L": ClassForm.講義,
            "S": ClassForm.演習,
            "E": ClassForm.実験,
            "P": ClassForm.実習,
            "T": ClassForm.卒業論文,
            "Z": ClassForm.その他,
        }[self[10]]

    @property
    def language(self) -> Language:
        return {
            1: Language.Japanese,
            2: Language.JapaneseAndEnglish,
            3: Language.English,
            4: Language.OtherLanguagesToo,
            5: Language.OnlyOtherLanguages,
            9: Language.Others,
        }[int(self[11])]


class SearchResultItem(NamedTuple):
    """Summary of a course in search results. Call `fetch_details` to get more information."""

    時間割コード: str
    共通科目コード: CommonCode
    コース名: str
    教員: str
    学期: set[Semester]
    曜限: set[tuple[Weekday, int]]
    ねらい: str


class SearchResult(NamedTuple):
    """Result of a search query."""

    items: list[SearchResultItem]
    current_items_first_index: int
    current_items_last_index: int
    current_items_count: int
    total_items_count: int
    current_page: int
    total_pages: int


class Details(NamedTuple):
    """Details of a course. Contains all available information for a course on the website. (UTAS may have more information)"""

    時間割コード: str
    共通科目コード: CommonCode
    コース名: str
    教員: str
    学期: set[Semester]
    曜限: set[tuple[Weekday, int]]
    ねらい: str
    教室: str
    単位数: Decimal
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

    def id(self) -> str:
        return hashlib.sha256(str(self).encode()).hexdigest()


def parse_department(faculty: Faculty, department_code: str):
    d = {
        Faculty.教養学部前期課程: {
            "": """
基礎科目 Foundation Courses FC
展開科目 Intermediate Courses IC
総合科目 Integrated Courses GC
主題科目 Thematic Courses TC
基礎科目
(PEAK)
Foundation Courses
(PEAK)
PF
展開科目
(PEAK)
Intermediate Courses
(PEAK)
PI
総合科目
(PEAK)
Integrated Courses
(PEAK)
PG
主題科目
(PEAK)
Thematic Courses
(PEAK)
PT""",
            "FC": "基礎科目",
            "IC": "展開科目",
            "GC": "総合科目",
            "TC": "主題科目",
            "PF": "基礎科目(PEAK)",
            "PI": "展開科目(PEAK)",
            "PG": "総合科目(PEAK)",
            "PT": "主題科目(PEAK)",
        },
        Faculty.法学部: {
            "": """学科等名 学科等名（英語） 「開講学科等」コード
共通科目 Common ＣＯ
実定法系科目 Positive Law ＰＬ
基礎法学系科目 Basic Law ＢＬ
政治系科目 Political Science ＰＳ
経済系科目 Economics ＥＣ
演習科目 Seminar ＳＥ""",
            "CO": "共通科目",
            "PL": "実定法系科目",
            "BL": "基礎法学系科目",
            "PS": "政治系科目",
            "EC": "経済系科目",
            "SE": "演習科目",
        },
        Faculty.医学部: {"ME": "医学科", "IE": "健康総合科学科"},
        Faculty.工学部: {
            "": """学科等名 学科等名（英語）
共通科目 Common Courses
日本語教育部門 Japanese Language Class
社会基盤学科 Department of Civil Engineering
建築学科 Department of Architecture
都市工学科 Department of Urban Engineering
機械系
機械工学科 Department of Mechanical Engineering
機械情報工学科 Department of Mechano-Informatics
航空宇宙工学科 Department of Aeronautics and Astronautics
精密工学科 Department of Precision Engineering
電子・情報系
Department of Electrical and Electronic Engineering,
Department of Information and Communication
Engineering
応用物理系
物理工学科 Department of Applied Physics
計数工学科
Department of Mathematical Engineering and
Infomation Physics
マテリアル工学科 Department of Materials Engineering
化学・生命系 Chemistry and Biotechnology Departments
応用化学科 Department of Applied Chemistry
化学システム工学科 Department of Chemical System Engineering
化学生命工学科 Department of Chemistry and Biotechnology
システム創成学科 Department of Systems Innovation
環境・エネルギーシステムコース Systems Innovation A(E&E)
システムデザイン＆マネジメントコース Systems Innovation B(SDM)
知能社会システムコース Systems Innovation C(PSI)
コード
CO
JL
CE
AR
UE
MX
ME
MI
AA
PE
EE
AM
AP
MP
MA
CH
CA
CS
CB
SI
SA
SB
SC
""",
            "CO": "共通科目",
            "JL": "日本語教育部門",
            "CE": "社会基盤学科",
            "AR": "建築学科",
            "UE": "都市工学科",
            "MX": "機械系",
            "ME": "機械工学科",
            "MI": "機械情報工学科",
            "AA": "航空宇宙工学科",
            "PE": "精密工学科",
            "EE": "電子・情報系",
            "AM": "応用物理系",
            "AP": "物理工学科",
            "MP": "計数工学科",
            "MA": "マテリアル工学科",
            "CH": "化学・生命系",
            "CA": "応用化学科",
            "CS": "化学システム工学科",
            "CB": "化学生命工学科",
            "SI": "システム創成学科",
            "SA": "環境・エネルギーシステムコース",
            "SB": "システムデザイン＆マネジメントコース",
            "SC": "知能社会システムコース",
        },
        Faculty.文学部: {
            "": """人文学科 Humanities HU
専修課程以外 XX""",
            "HU": "人文学科",
            "XX": "専修課程以外",
        },
        Faculty.理学部: {
            "MA": "数学科",
            "IS": "情報科学科",
            "PH": "物理学科",
            "AS": "天文学科",
            "EP": "地球惑星物理学科",
            "EE": "地球惑星環境学科",
            "CH": "化学科",
            "BC": "生物化学科",
            "BS": "生物学科",
            "BI": "生物情報科学科",
            "CC": "理学部共通科目",
        },
        Faculty.農学部: {
            "": """MC 0 生命化学・工学専修 Major in Biological Chemistry and

Biotechnology

MB 0 応用生物学専修 Major in Applied Biology
MF 0

森林生物科学専修/
森林環境資源科学専修

Major in Forest Life Science/
Major in Forest Environmental and Resource
Science

MQ 0 水圏生物科学専修 Major in Aquatic Bioscience
MA 0 動物生命システム科学専修 Major in Animal Life Sciences
MM 0 生物素材科学専修 Major in Biobased Materials Chemistry
ML 0 緑地環境学専修 Major in Landscape Ecology and Planning
MW 0 木質構造科学専修 Major in Wood Science and Timber

Engineering

MG 0 生物・環境工学専修 Major in Biological and Environmental

Engineering

ME 0 農業・資源経済学専修 Major in Agricultural and Resource

Economics

MS 0 フィールド科学専修 Major in Field Science
MI 0 国際開発農学専修 Major in International Sustainable

Agriculture Development

MV 0 獣医学専修 Major in Veterinary Medical Sciences
            """,
            "MC": "生命化学・工学専修",
            "MB": "応用生物学専修",
            "MF": "森林生物科学専修/森林環境資源科学専修",
            "MQ": "水圏生物科学専修",
            "MA": "動物生命システム科学専修",
            "MM": "生物素材科学専修",
            "ML": "緑地環境学専修",
            "MW": "木質構造科学専修",
            "MG": "生物・環境工学専修",
            "ME": "農業・資源経済学専修",
            "MS": "フィールド科学専修",
            "MI": "国際開発農学専修",
            "MV": "獣医学専修",
        },
        Faculty.経済学部: {
            "": """学科等名 学科等名（英語） 「開講学科等」コード
経済学 Economics ＥＣ
統計学 Statistics ＳＴ
地域研究 Area Studies ＡＳ
経済史 Economic History ＥＨ
経営学 Management ＭＡ
数量ファイナンス Quantitative Finance ＱＦ
その他 Others ＷＷ""",
            "EC": "経済学",
            "ST": "統計学",
            "AS": "地域研究",
            "EH": "経済史",
            "MA": "経営学",
            "QF": "数量ファイナンス",
            "WW": "その他",
        },
        Faculty.教養学部: {
            "": """学科等名 学科等名（英語） 「開講学科等」コード
言語共通科目 General Language Courses AA
言語専門科目 Language Courses for Specific Disciplines BA
教養学科 Department of Humanities and Social Sciences CA
学際科学科 Department of Interdisciplinary Sciences DA
統合自然科学科 Department of Integrated Sciences EA
学融合プログラム Cross‐disciplinary Programs FA
教職科目 Pedagogy Program Courses GA
特設科目
Special Courses in Japan and Asian Studies
and others
HA
高度教養科目 Liberal Arts Courses for Advanced Students XA""",
            "AA": "言語共通科目",
            "BA": "言語専門科目",
            "CA": "教養学科",
            "DA": "学際科学科",
            "EA": "統合自然科学科",
            "FA": "学融合プログラム",
            "GA": "教職科目",
            "HA": "特設科目",
            "XA": "高度教養科目",
        },
        Faculty.教育学部: {
            "": """学科等名 学科等名（英語） 「開講学科等」コード
総合教育科学科
Integrated Educational
Sciences
ＩＥ
基礎教育学コース
Division of Basic
Theories of Education
ＢＴ
教育社会科学専修
Social Sciences in
Education
ＳＳ
（教育社会科学専修）
比較教育社会学コース
Division of Social
Sciences in Education
ＳＯ
（教育社会科学専修）
教育実践・政策学コース
Division of Educational
Practices and Policies
ＰＰ
心身発達科学専修
Developmental
Sciences
ＤＳ
（心身発達科学専修）
教育心理学コース
Division of Educational
Psychology
ＥＰ
（心身発達科学専修）
身体教育学コース
Division of Phycical and
Health Education
ＰＨ""",
            "IE": "総合教育科学科",
            "BT": "基礎教育学コース",
            "SS": "教育社会科学専修",
            "SO": "比較教育社会学コース",
            "PP": "教育実践・政策学コース",
            "DS": "心身発達科学専修",
            "EP": "教育心理学コース",
            "PH": "身体教育学コース",
        },
        Faculty.薬学部: {
            "": """Department Name in English Code
薬科学科／薬学科 Pharmaceutical Sciences／Pharmacy ＳＨ
薬科学科 Pharmaceutical Sciences ＰＳ
薬学科 Pharmacy ＰＨ""",
            "SH": "薬科学科／薬学科",
            "PS": "薬科学科",
            "PH": "薬学科",
        },
        Faculty.理学系研究科: {
            "PH": "物理学専攻",
            "AS": "天文学専攻",
            "EP": "地球惑星科学専攻",
            "EE": "地球惑星環境学科",
            "CH": "化学専攻",
            "BC": "生物化学科",
            "BS": "生物科学専攻",
            "BI": "生物情報科学科",
            "CC": "理学部共通科目",
        },
        Faculty.人文社会系研究科: {
            "": """専攻等名 専攻等名（英語） 「開講専攻等」コード
総合教育科学専攻
Department of
Integrated
Educational
IE
学校教育高度化専攻
Department of
Advanced Research
in School Education
AS
その他 Others ZZ
""",
            "IE": "総合教育科学専攻",
            "AS": "学校教育高度化専攻",
            "ZZ": "その他",
        },
        Faculty.法学政治学研究科: {
            "": """専攻等名 専攻等名（英語） 「開講専攻等」コード
総合法政専攻 School of Legal and Political Studies ＬＰ
法曹養成専攻 School of Law ＬＳ""",
            "LP": "総合法政専攻",
            "LS": "法曹養成専攻",
        },
        Faculty.経済学研究科: {
            "EC": "経済学研究科",
        },
        Faculty.総合文化研究科: {
            "": """専攻等名 専攻等名（英語） 「開講専攻等」コード
言語情報科学専攻 Language and Information
Sciences LI
超域文化科学専攻 Interdisciplinary Cultural
Studies IC
地域文化研究専攻 Area Studies AS
国際社会科学専攻 Advanced Social and
International Studies SI
広域科学専攻
生命環境科学系
Multidisciplinary Sciences:
Life Sciences LS
広域科学専攻
広域システム科学系
Multidisciplinary Sciences:
General Systems Studies SS
広域科学専攻
相関基礎科学系
Multidisciplinary Sciences:
Basic Science BS
「人間の安全保障」プログラム Graduate Program on Human
Security HS
欧州研究プログラム European Studies Program EU
グローバル共生プログラム Graduate Program on Global
Humanities GH
多文化共生・統合人間学
プログラム
Integrated Human Sciences
Program for Cultural
Diversity
IH
国際人材養成プログラム Graduate Program on Global
Society GS
国際環境学プログラム Graduate Program on
Environmental Sciences ES
グローバル・スタディーズ・
イニシアティヴ国際卓越大学院
Global Studies Initiative
World-leading Innovative
Graduate Study Program
GW
先進基礎科学推進国際卓越
大学院
World-leading Innovative
Graduate Study Program of
Advanced Basic Science Course
(WINGS-ABC)
WA
科学技術インタープリター養成
プログラム
Science Interpreter Training
Program IT
日独共同大学院プログラム Internationales
Graduiertenkolleg IG
英語教育プログラム English Education Program EE""",
            "LI": "言語情報科学専攻",
            "IC": "超域文化科学専攻",
            "AS": "地域文化研究専攻",
            "SI": "国際社会科学専攻",
            "LS": "広域科学専攻 生命環境科学系",
            "SS": "広域科学専攻 広域システム科学系",
            "BS": "広域科学専攻 相関基礎科学系",
            "HS": "「人間の安全保障」プログラム",
            "EU": "欧州研究プログラム",
            "GH": "グローバル共生プログラム",
            "IH": "多文化共生・統合人間学プログラム",
            "GS": "国際人材養成プログラム",
            "ES": "国際環境学プログラム",
            "GW": "グローバル・スタディーズ・イニシアティヴ国際卓越大学院",
            "WA": "先進基礎科学推進国際卓越大学院",
            "IT": "科学技術インタープリター養成プログラム",
            "IG": "日独共同大学院プログラム",
            "EE": "英語教育プログラム",
        },
        Faculty.工学系研究科: {"": ""},
        Faculty.農学生命科学研究科: {
            "": """専攻等名 専攻等名（英語） 「開講専攻等」コード
共通 Common Course ＣＣ
生産・環境生物学 Agricultural and Environmental
Biology AB
応用生命化学 Applied Biological Chemistry AC
応用生命工学 Biotechnology BT
森林科学 Forest Science FS
水圏生物科学 Aquatic Bioscience AQ
農業・資源経済学 Agricultural and Recource
Economics AE
生物・環境工学 Biological and Environmental Engineering BE
生物材料科学 Biomaterial Sciences BM
生物材料科学・木造建築コース WA
農学国際 Global Agricultural Sciences GA
農学国際・国際農業開発学コース International Program in
Agricultural Development IP
生圏システム学 Ecosystem Studies ES
応用動物科学 Animal Resource Sciences AS
獣医学 Veterinary Medical Sciences VM
副専攻 Minor Subject MS
""",
            "CC": "共通",
            "AB": "生産・環境生物学",
            "AC": "応用生命化学",
            "BT": "応用生命工学",
            "FS": "森林科学",
            "AQ": "水圏生物科学",
            "AE": "農業・資源経済学",
            "BE": "生物・環境工学",
            "BM": "生物材料科学",
            "WA": "生物材料科学・木造建築コース",
            "GA": "農学国際",
            "IP": "農学国際・国際農業開発学コース",
            "ES": "生圏システム学",
            "AS": "応用動物科学",
            "VM": "獣医学",
            "MS": "副専攻",
        },
        Faculty.医学系研究科: {"ME": "医学系研究科"},
        Faculty.薬学系研究科: {
            "": """専攻等名 専攻等名（英語） 「開講専攻等」コード
薬科学専攻／薬
学専攻
Pharmaceutical
Sciences/Pharmacy ＳＨ
薬科学専攻 Pharmaceutical
Sciences ＰＳ
薬学専攻 Pharmacy ＰＨ
生命科学技術国
際卓越大学院 WINGS-LST ＷＬ""",
            "SH": "薬科学専攻／薬学専攻",
            "PS": "薬科学専攻",
            "PH": "薬学専攻",
            "WL": "生命科学技術国際卓越大学院 WINGS-LST",
        },
        Faculty.数理科学研究科: {"MA": "数理科学研究科"},
        Faculty.情報理工学系研究科: {
            "": """専攻等名 専攻等名（英語） 開講専攻等コード
コンピュータ科学 Computer Science ＣＳ
数理情報学 Mathematical Informatics ＭＡ
システム情報学 Information Physics & Computing ＩＰ
電子情報学
Information & Communication
Engineering
ＩＣ
知能機械情報学 Mechano-Informatics ＭＸ
創造情報学 Creative Informatics ＣＩ
共通科目 Common Courses ＣＯ""",
            "CS": "コンピュータ科学",
            "MA": "数理情報学",
            "IP": "システム情報学",
            "IC": "電子情報学",
            "MX": "知能機械情報学",
            "CI": "創造情報学",
            "CO": "共通科目",
        },
        Faculty.新領域創成科学研究科: {
            "": """専攻等名 専攻等名（英語） 「開講専攻等」コード
全学開放科目 University-wide Open
Courses OC
新領域創成科学研究科共通科目 Common Courses CC
環境学研究系共通科目 Division of Environmental
Studies Common Subjects EC
物質系専攻 Department of Advanced
Materials Science AM
先端エネルギー工学専攻 Department of Advanced
Energy AE
複雑理工学専攻 Department of Complexity
Science and Engineering CS
先端生命科学専攻 Department of Integrated
Biosciences IB
メディカル情報生命専攻
Department of
Computational Biology
and Medical Sciences
MJ
自然環境学専攻 Department of Natural
Environmental Studies NE
海洋技術環境学専攻
Department of Ocean
Technology, Policy, and
Environment
OT
環境システム学専攻 Department of
Environment Systems ES
人間環境学専攻
Department of Human and
Engineered Environmental
Studies
HE
社会文化環境学専攻
Department of SocioCultural Environmental
Studies
SC
国際協力学専攻 Department of
International Studies IS
サステイナビリティ学グローバル
リーダー養成大学院プログラム
Graduate Program in
Sustainability Science -
Global Leadership
Initiative
SS""",
            "OC": "全学開放科目",
            "CC": "新領域創成科学研究科共通科目",
            "EC": "環境学研究系共通科目",
            "AM": "物質系専攻",
            "AE": "先端エネルギー工学専攻",
            "CS": "複雑理工学専攻",
            "IB": "先端生命科学専攻",
            "MJ": "メディカル情報生命専攻",
            "NE": "自然環境学専攻",
            "OT": "海洋技術環境学専攻",
            "ES": "環境システム学専攻",
            "HE": "人間環境学専攻",
            "SC": "社会文化環境学専攻",
            "IS": "国際協力学専攻",
            "SS": "サステイナビリティ学グローバルリーダー養成大学院プログラム",
        },
        Faculty.学際情報学府: {
            "": """専攻等名（コース） 専攻等名（英語） 「開講専攻等」コード
社会情報学コース
Socio-information and
communication studies
course
ＳＣ
文化・人間情報学コース Cultural and human
information studies course ＣＨ
先端表現情報学コース Emerging design and
informatics course ＥＤ
総合分析情報学コース Applied computer science
course
ＡＣ
アジア情報社会コース ITASIA program ＩＡ
生物統計情報学コース Biostatistics and
bioinformatics course ＢＳ
学際情報学専攻（必修）
the Department of
Interdisciplinary Information
Studies（Required　subject）
ＲＳ
学際情報学専攻（共通）
the Department of
Interdisciplinary Information
Studies(Common subject)
ＣＳ
学際情報学専攻（横断）
the Department of
Interdisciplinary Information
Studies(Wide subject)
ＷＳ""",
            "SC": "社会情報学コース",
            "CH": "文化・人間情報学コース",
            "ED": "先端表現情報学コース",
            "AC": "総合分析情報学コース",
            "IA": "アジア情報社会コース",
            "BS": "生物統計情報学コース",
            "RS": "学際情報学専攻（必修）",
            "CS": "学際情報学専攻（共通）",
            "WS": "学際情報学専攻（横断）",
        },
        Faculty.公共政策学教育部: {
            "": """専攻等名
/Department
「開講専攻等」コード
/Department Code
国際公共政策学専攻 ＤＰ
公共政策学専攻 ＭＰ
""",
            "DP": "国際公共政策学専攻",
            "MP": "公共政策学専攻",
        },
    }
    return d[faculty].get(department_code, department_code)


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
    # if period_text == "集中":
    # Most complex case:"S1: 集中、A1: 月曜3限 他"
    if ":" in period_text:
        return set()
    # Ignore others if period_text contains "集中"
    if "集中" in period_text:
        return set()
    period_texts = period_text.split("、")

    def parse_one(period: str):
        w = Weekday([weekday in period for weekday in list("月火水木金土日")].index(True))
        reres = re.search(r"\d+", period)
        if not reres:
            # raise ValueError(f"Invalid period: {period}")
            return None
        p = int(reres.group())
        return w, p

    result = set()
    for item in period_texts:
        if not item:
            return set()
        result.add(parse_one(item))
    return result


async def await_if_future(obj: object) -> object:
    if isawaitable(obj):
        return await obj
    return obj


class ParserError(Exception):
    pass


from datetime import timedelta

from .common import RateLimitter


class UTCourseCatalog:
    """A parser for the [UTokyo Online Course Catalogue](https://catalog.he.u-tokyo.ac.jp)."""

    session: Optional[aiohttp.ClientSession]
    _logger: Logger
    _rate_limitter: RateLimitter

    def __init__(
        self, logger_level: int = 0, min_interval: Union[timedelta, int] = 1
    ) -> None:
        self.session = None
        self._logger = getLogger(__name__)
        self._logger.setLevel(logger_level)
        self._rate_limitter = RateLimitter(min_interval=min_interval)

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
        await self._rate_limitter.wait()
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
                        共通科目コード=CommonCode(code_cell_children[3].text),
                        コース名=get_cell_text("name"),
                        教員=get_cell_text("lecturer"),
                        学期=set(
                            [
                                Semester(el.text.replace(" ", "").replace("\n", ""))
                                for el in get_cell("semester").find_all(
                                    class_="catalog-semester-icon"
                                )
                            ]
                        ),
                        曜限=set(_parse_weekday_period(get_cell_text("period"))),
                    )

            items = list(get_items())
            if page != total_pages:
                if len(items) != 10:
                    raise ParserError("items count is not 10")
                if len(items) != current_items_count:
                    raise ParserError("items count is not current_items_count")
            if page != current_items_first_index // 10 + 1:
                raise ParserError("page number is not correct")

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

        await self._rate_limitter.wait()
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
                card = get_card(name)
                if card:
                    return _format_description(card.text)
                return None

            code_cell = _ensure_found(cells1_parent.find(class_="code-cell"))
            code_cell_children = list(code_cell.children)

            # return the result
            return Details(
                時間割コード=code_cell_children[1].text,
                共通科目コード=CommonCode(code_cell_children[3].text),
                コース名=get_cell1("name"),
                教員=get_cell1("lecturer"),
                学期=set(
                    [
                        Semester(el.text.replace(" ", "").replace("\n", ""))
                        for el in cells1_parent.find_all(class_="catalog-semester-icon")
                    ]
                ),
                曜限=_parse_weekday_period(get_cell1("period")),
                教室=get_cell2(0),
                単位数=Decimal(get_cell2(1)),
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

    async def fetch_common_code(self, 時間割コード: str) -> CommonCode:
        """Fetch common code of a course from its time table code.

        Returns
        -------
        CommonCode
            Common code of the course
        """
        result = await self.fetch_search(SearchParams(keyword=時間割コード))
        return result.items[0].共通科目コード

    async def fetch_code(self, 共通科目コード: str) -> str:
        """Fetch time table code of a course from its common code.

        Returns
        -------
        str
            Time table code of the course
        """
        result = await self.fetch_search(SearchParams(keyword=共通科目コード))
        return result.items[0].時間割コード

    def retry(self, func: WrappedFn) -> WrappedFn:
        return retry(
            stop=(stop_after_delay(10) | stop_after_attempt(3)),
            wait=wait_exponential(multiplier=1, min=4, max=16),
            before_sleep=before_sleep_log(self._logger, 30),
        )(func)

    async def fetch_search_all(
        self,
        params: SearchParams,
        *,
        use_tqdm: bool = True,
        on_initial_request: Optional[
            Callable[[SearchResult], Optional[Awaitable]]
        ] = None,
    ) -> AsyncIterable[SearchResultItem]:
        """Fetch all search results by repeatedly calling `fetch_search`.

        Parameters
        ----------
        params : SearchParams
            Search parameters
        use_tqdm : bool, optional
            Whether to use tqdm, by default True
        on_initial_request : Optional[Callable[[SearchResult], Optional[Awaitable]]], optional
            Callback function to be called on the initial request, by default None

        Returns
        -------
        AsyncIterable[SearchResultItem]
            Async iterable of search results

        Yields
        ------
        Iterator[AsyncIterable[SearchResultItem]]
            Async iterable of search results
        """
        pbar = tqdm(disable=not use_tqdm)
        result = await self.fetch_search(params)
        pbar.update()

        if on_initial_request:
            await await_if_future(on_initial_request(result))

        for item in result.items:
            yield item

        pbar.total = result.total_pages
        tasks = []
        for page in range(2, result.total_pages + 1):

            async def inner(page):
                try:
                    search = await self.retry(self.fetch_search)(params, page)
                except:
                    self._logger.error(f"Failed to fetch page {page}")
                    return None
                pbar.update(1)
                return search

            result_task = create_task(inner(page))
            tasks.append(result_task)
        results = await asyncio.gather(*tasks)
        for result in results:
            if result:
                for item in result.items:
                    yield item

    async def fetch_search_detail_all(
        self,
        params: SearchParams,
        *,
        year: int = 2022,
        use_tqdm: bool = True,
        on_initial_request: Optional[
            Callable[[SearchResult], Optional[Awaitable]]
        ] = None,
        on_detail_request: Optional[Callable[[Details], Optional[Awaitable]]] = None,
    ) -> Iterable[Details]:
        """Fetch all search results by repeatedly calling `fetch_search` and `fetch_detail`.

        Parameters
        ----------
        params : SearchParams
            Search parameters
        year : int, optional
            Year of the course, by default 2022
        use_tqdm : bool, optional
            Whether to use tqdm, by default True
        on_initial_request : Optional[Callable[[SearchResult], Optional[Awaitable]]], optional
            Callback function to be called on the initial request, by default None

        Returns
        -------
        AsyncIterable[Details]
            Async iterable of details

        Yields
        ------
        Iterator[AsyncIterable[Details]]
            Async iterable of details
        """

        pbar = tqdm(disable=not use_tqdm)

        async def on_initial_request_wrapper(search_result: SearchResult):
            pbar.total = search_result.total_items_count
            if on_initial_request:
                await await_if_future(on_initial_request(search_result))

        tasks = []
        items = [
            item
            async for item in self.fetch_search_all(
                params,
                use_tqdm=True,
                on_initial_request=on_initial_request_wrapper,
            )
        ]
        s = asyncio.Semaphore(100)
        for item in items:

            async def inner(item):
                async with s:
                    try:
                        details = await self.retry(self.fetch_detail)(item.時間割コード, year)
                    except Exception as e:
                        self._logger.error(e)
                        return None
                    pbar.update()
                    if on_detail_request:
                        await await_if_future(on_detail_request(details))
                    return details

            detail_task = create_task(inner(item))
            tasks.append(detail_task)
        results = await asyncio.gather(*tasks)
        return results

    async def fetch_and_save_search_detail_all(
        self,
        params: SearchParams,
        *,
        year: int = 2022,
        filename: Optional[str] = None,
        use_tqdm: bool = True,
        on_initial_request: Optional[
            Callable[[SearchResult], Optional[Awaitable]]
        ] = None,
    ) -> Iterable[Details]:
        """Fetch all search results by repeatedly calling `fetch_search` and `fetch_detail` and save them to a PKL file.
        The filename is params.id() + ".pkl" if not specified.

        Parameters
        ----------
        params : SearchParams
            Search parameters
        year : int, optional
            Year of the course, by default 2022
        filename : Optional[str], optional
            Filename to save the results, by default None. If None, the filename is params.id() + ".pkl".
        use_tqdm : bool, optional
            Whether to use tqdm, by default True
        on_initial_request : Optional[Callable[[SearchResult], Optional[Awaitable]]], optional
            Callback function to be called on the initial request, by default None

        Returns
        -------
        AsyncIterable[Details]
            Async iterable of details

        Yields
        ------
        Iterator[AsyncIterable[Details]]
            Async iterable of details
        """
        if not filename:
            filename = params.id()
        if not filename.endswith(".pkl"):
            filename += ".pkl"
        filepath = Path(filename)
        self._logger.info(f"Saving to {filepath}")
        result = await self.fetch_search_detail_all(
            params,
            year=year,
            use_tqdm=use_tqdm,
            on_initial_request=on_initial_request,
        )
        try:
            async with aiofiles.open(filename, "wb") as f:
                await f.write(pickle.dumps(result))
        except Exception as e:
            self._logger.error(e)
            self._logger.error(f"Skipping saving to {filepath}")
        return result

    async def fetch_and_save_search_detail_all_pandas(
        self,
        params: SearchParams,
        *,
        year: int = 2022,
        filename: Optional[str] = None,
        use_tqdm: bool = True,
        on_initial_request: Optional[
            Callable[[SearchResult], Optional[Awaitable]]
        ] = None,
    ) -> DataFrame:
        data = await self.fetch_and_save_search_detail_all(
            params,
            year=year,
            use_tqdm=use_tqdm,
            on_initial_request=on_initial_request,
            filename=filename,
        )
        try:
            from .pandas import to_dataframe

            df = to_dataframe(data)
        except Exception as e:
            self._logger.error(e)
            self._logger.error("Returning raw data instead of pandas dataframe.")
            return data  # type: ignore
        try:
            if not filename:
                filename = params.id()
            if not filename.endswith(".pkl"):
                filename += ".pandas.pkl"
            else:
                filename = filename.replace(".pkl", ".pandas.pkl")
            filepath = Path(filename)
            self._logger.info(f"Saving to {filepath}")
            df.to_pickle(filename)
        except Exception as e:
            self._logger.error(e)
            self._logger.error(f"Skipping saving to {filename}")
        return df

    def read_pandas(self, params) -> DataFrame:
        import pandas as pd

        filename = params.id() + "_pandas.pkl"
        return pd.read_pickle(filename)
