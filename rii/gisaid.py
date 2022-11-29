import fnmatch
from collections import defaultdict
from pathlib import Path
from typing import Generator, Iterable, Optional

import pandas as pd

from rii.loaders import iter_chunks_from_tar_or_csv

aa_substitution_pattern = (
    r"(?P<gene>[A-Za-z\d]+)_(?P<ref>[A-Za-z]+)(?P<pos>\d+)(?P<seq>[A-Za-z]+)"
)

METADATA_DTYPES = defaultdict(
    pd.StringDtype,
    {
        "Sequence length": pd.UInt32Dtype(),
        "Is reference?": pd.BooleanDtype(),
        "Is complete?": pd.BooleanDtype(),
        "Is high coverage?": pd.BooleanDtype(),
        "Is low coverage?": pd.BooleanDtype(),
        "N-Content": pd.Float32Dtype(),
        "GC-Content": pd.Float32Dtype(),
    },
)


def iter_metadata(
    file: Path, chunksize: int = 100_000
) -> Generator[pd.DataFrame, None, None]:
    yield from iter_chunks_from_tar_or_csv(
        file,
        tar_member="metadata.tsv",
        sep="\t",
        dtype=METADATA_DTYPES,
        chunksize=chunksize,
    )


def make_query(
    location: Optional[Iterable[str]] = None,
    pango_lineage: Optional[Iterable[str]] = None,
) -> str:
    query = []
    if location:
        query.append(
            "("
            + " or ".join(f"Location.str.contains('{loc}')" for loc in location)
            + ")"
        )
    if pango_lineage:
        query.append(
            "("
            + " or ".join(
                f"`Pango lineage`.str.fullmatch('{fnmatch.translate(pl)}')"
                for pl in pango_lineage
            )
            + ")"
        )

    return " and ".join(query)
