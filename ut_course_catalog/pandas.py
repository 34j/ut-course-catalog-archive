from typing import Iterable, NamedTuple, Optional
import pandas as pd
from enum import Enum, auto
from wordcloud import WordCloud


def to_series(item: NamedTuple) -> pd.Series:
    return pd.Series(item._asdict())


def to_dataframe(items: Iterable[NamedTuple]) -> pd.DataFrame:
    return pd.DataFrame([x._asdict() for x in items if x])


def create_wordcloud(params, txt, *, size=(1200, 900), **kwargs):
    from janome.tokenizer import Tokenizer
    from wordcloud import WordCloud

    t = Tokenizer()
    tokens = t.tokenize(txt)
    words = {}
    for token in tokens:
        data = str(token).split()[1].split(",")
        if data[0] == "名詞":
            key = data[6]
            if key not in words:
                words[key] = 1
            else:
                words[key] += 1

    font_path = "C:\\Windows\\Fonts\\msgothic.ttc"
    wordcloud = WordCloud(
        font_path=font_path, width=size[0], height=size[1], **kwargs
    ).generate_from_frequencies(words)
    wordcloud.to_file(
        filename="analysis/"
        + "".join(
            [
                str(v.name) if isinstance(v, Enum) else str(v)
                for k, v in params.__dict__.items()
                if v
            ]
        )
        + "_wordcloud.jpg"
    )


class ScoringMethod(Enum):
    中間 = auto()
    期末 = auto()
    小テスト = auto()
    演習 = auto()
    課題 = auto()
    レポート = auto()
    発表 = auto()
    出席 = auto()


def _in_any(items: Iterable, text: str) -> bool:
    return any([item in text for item in items])


def parse_scoring_method(text: Optional[str]) -> set[ScoringMethod]:
    d = {
        ScoringMethod.中間: ["中間", "mid"],
        ScoringMethod.期末: ["試験", "exam", "テスト", "最終試験", "追試", "Makeup"],
        ScoringMethod.小テスト: ["小テスト", "クイズ", "quiz"],
        ScoringMethod.演習: ["演習", "実習"],
        ScoringMethod.課題: ["課題", "assign", "宿題"],
        ScoringMethod.レポート: ["レポート", "レポ", "report"],
        ScoringMethod.発表: ["発表", "presenta", "プレゼン"],
        ScoringMethod.出席: ["出席", "発表", "参加", "attend", "平常", "出欠", "リアペ", "リアクション"],
    }
    result = set()
    if text is None:
        return result
    for k, v in d.items():
        if _in_any(v, text):
            result.add(k)
    if "期末" in text and not _in_any(["期末レポ", "期末課題"], text):
        result.add(ScoringMethod.期末)

    return result


def encode_scoring_method(texts: "pd.Series[str]") -> pd.DataFrame:
    methods = texts.apply(lambda x: list(parse_scoring_method(x)))
    columns = []
    for method in ScoringMethod:
        column = methods.apply(lambda x: method in x)
        column.name = method.name
        columns.append(column)
    df = pd.concat(columns, axis=1)
    df = df.astype(int)
    return df


def encode_common_code(common_codes: "pd.Series[CommonCode]") -> pd.DataFrame:
    return pd.concat([common_codes.apply(lambda x: x.department)], axis=1)