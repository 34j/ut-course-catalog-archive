from unittest import IsolatedAsyncioTestCase
import ut_course_catalog.ja as utcc
from ut_course_catalog import Weekday
from rich.console import Console
import pandas as pd


class TestJa(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.console = Console()
        self.catalog = utcc.UTCourseCatalog()
        await self.catalog.__aenter__()

    async def asyncTearDown(self) -> None:
        await self.catalog.__aexit__(None, None, None)

    async def test_detail(self) -> None:
        detail = await self.catalog.fetch_detail("060320623", 2022)
        #self.console.print(detail)

    async def test_search(self) -> None:
        results = await self.catalog.fetch_search(
            utcc.SearchParams(keyword="量子力学", 曜日=[Weekday.Mon])
        )
        #self.console.print(results)
        df = pd.DataFrame([x._asdict() for x in results.items])
        self.assertTrue(df["曜限"].str.contains("Mon").all())
