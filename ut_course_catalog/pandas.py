import pandas as pd
from typing import Iterable, NamedTuple, Optional


def to_series(item: NamedTuple) -> pd.Series:
    return pd.Series(item._asdict())


def to_dataframe(items: Iterable[NamedTuple]) -> pd.DataFrame:
    return pd.DataFrame([x._asdict() for x in items if x])
