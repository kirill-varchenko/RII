from functools import partial
from typing import Any, Optional

import pandas as pd
import pdpipe as pdp
import thefuzz.fuzz
import thefuzz.process
import yaml
from pdpipe.col_generation import ColumnTransformer
from pdpipe.core import (ColumnsBasedPipelineStage, PdpApplicationContext,
                         PdPipelineStage)


class Lookup(ColumnTransformer):
    def __init__(
        self,
        column: str,
        lookup: dict[str, Any],
        default: Any = None,
        passthrough: bool = False,
        result_column: Optional[str] = None,
        drop: bool = True,
        suffix: str = "_lookup",
    ):
        self._lookup = lookup
        self._default = default
        self._passthrough = passthrough
        self._lookup_get = (
            (lambda v: self._lookup.get(v, self._default))
            if not self._passthrough
            else (lambda v: self._lookup.get(v, v))
        )
        super().__init__(
            column, result_column, drop, suffix, desc=f"Lookup column {column!r}"
        )

    def _col_transform(self, series: pd.Series, label: str) -> pd.Series:
        if not self._passthrough:
            messages = self.application_context.get("messages", [])
            messages.append(
                series[
                    ~series.isin(self._lookup) & series.str.len().gt(0)
                ].drop_duplicates()
            )
            self.application_context["messages"] = messages
        return series.apply(self._lookup_get)


class FuzzySearch(ColumnsBasedPipelineStage):
    def __init__(
        self,
        column: str,
        choices: list[str],
        result_column: Optional[str] = None,
        suffixes: tuple[str, str] = ("_fuzz", "_score"),
        keep_score: bool = True,
        scorer=None,
    ):
        self._column = column
        self._choices = choices
        self._result_column = result_column or column
        self._match_column = self._result_column + suffixes[0]
        self._score_column = self._result_column + suffixes[1]
        self._keep_score = keep_score
        self._scorer = scorer or thefuzz.process.default_scorer
        self._extract = partial(
            thefuzz.process.extractOne, choices=self._choices, scorer=self._scorer
        )
        super().__init__(column, desc=f"Fuzzy search with column {self._column!r}")

    def _transformation(self, df, verbose, fit):
        extracted = df[self._column].apply(self._extract).to_list()
        extracted_df = pd.DataFrame(
            extracted, index=df.index, columns=[self._match_column, self._score_column]
        )
        if not self._keep_score:
            extracted_df.drop(columns=self._score_column, inplace=True)
        return pd.concat([df, extracted_df], axis="columns")


class CollectMessages(PdPipelineStage):
    def __init__(self, result_column: str = "messages") -> None:
        self._result_column = result_column
        super().__init__(desc=f"Collect messages into column {result_column!r}")

    def _prec(self, df: pd.DataFrame) -> bool:
        return True

    def _transform(self, df, verbose):
        # print(self.application_context["messages"])
        df[self._result_column] = "message"
        return df


# data = pd.read_csv("rii/pcr_22-23.tsv", sep='\t', dtype=str)

# with open("rii/transform_lookups.yml", "r") as fi:
#     lookups = yaml.load(fi, Loader=yaml.Loader)

# pipe = pdp.PdPipeline(
#     [
#         Lookup("A", lu, passthrough=True, drop=False, result_column="B"),
#         FuzzySearch("A", choices=fl),
#         CollectMessages(),
#     ]
# )
# # print(pipe)
# # print(df)
# print(pipe(data))
