import warnings
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Generator, Literal

import pandas as pd
import pdpipe as pdp

BASE_DATE = date(1899, 12, 30)


def fix_190x_date(value: str) -> str:
    try:
        d = date.fromisoformat(value)
        if d.year in (1900, 1905):
            return str((d - BASE_DATE).days)
    except:
        pass
    return value


def stringify(value: Any) -> str:
    if value is None:
        return ""
    elif isinstance(value, str):
        return value
    elif isinstance(value, datetime):
        return str(value.date())
    elif isinstance(value, date):
        return str(value)
    return str(value)


def generate_concatenators(
    concatenate_scheme: dict[str, list[str]]
) -> Generator[tuple[str, Callable[[pd.Series], str]], None, None]:
    for new_col, old_cols in concatenate_scheme.items():
        yield new_col, lambda row: " ".join(row[col] for col in old_cols).strip()


def extract(
    table: Literal["gz", "pcr_21-22", "pcr_22-23"], file: Path, extract_schemes: dict
) -> pd.DataFrame:
    converters = {
        col: stringify for col in extract_schemes["tables"][table]["read"]["usecols"]
    }

    read_args = extract_schemes["tables"][table]["read"]
    read_args.update({"io": file, "converters": converters})

    warnings.simplefilter("ignore", UserWarning)
    data = pd.read_excel(**read_args)

    pipeline = pdp.PdPipeline(
        [
            pdp.df.fillna(""),
            pdp.df.applymap(fix_190x_date),
            pdp.df["source_id"] << table,
        ]
    )
    for new_col, concatenator in generate_concatenators(
        extract_schemes["tables"][table]["concat"]
    ):
        pipeline += pdp.ApplyToRows(concatenator, new_col)
    pipeline += pdp.df.replace(r"\s+", " ", regex=True)
    pipeline += pdp.df.applymap(str.strip)
    pipeline += pdp.ColRename(extract_schemes["tables"][table]["rename"])
    pipeline += pdp.Schematize(extract_schemes["columns"])

    return pipeline(data)
