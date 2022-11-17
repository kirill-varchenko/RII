#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import tarfile
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import IO, Generator

import pandas as pd

CHUNKSIZE = 100_000

METADATA_DTYPES = defaultdict(
    lambda: pd.StringDtype(),
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


def iter_chunks_from_tsv(file: IO[bytes]) -> Generator[pd.DataFrame, None, None]:
    yield from pd.read_csv(file, sep="\t", dtype=METADATA_DTYPES, chunksize=CHUNKSIZE)


def iter_chunks(file: Path) -> Generator[pd.DataFrame, None, None]:
    if file.suffix == ".xz":
        with tarfile.open(file) as tar:
            metadata_file = tar.extractfile("metadata.tsv")
            assert metadata_file is not None
            yield from iter_chunks_from_tsv(metadata_file)
    elif file.suffix == ".tsv":
        with open(file, "rb") as metadata_file:
            yield from iter_chunks_from_tsv(metadata_file)
    else:
        raise ValueError(file.suffix)


def parse_date_to_week(str_date: str) -> str:
    try:
        return "{0.year}-W{0.week:02}".format(
            date.fromisoformat(str_date).isocalendar()
        )
    except:
        return ""


def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    # Filtering
    chunk = chunk[
        chunk["Location"].str.contains("Russia")
        | chunk["Location"].str.contains("Crimea")
    ].copy()

    # Extract ISO
    chunk["ISO"] = chunk["Virus name"].str.extract(r"Russia/([A-Z]{1,3})-", expand=True)
    chunk.loc[chunk["Location"].str.contains("Crimea"), "ISO"] = "Crimea"

    # Set RII flag
    chunk["RII"] = chunk["Virus name"].str.contains("-RII-")

    # Process Collection date
    chunk["Collection month"] = chunk["Collection date"].str.extract(
        r"(\d{4}-\d{2})", expand=True
    )
    chunk["Collection week"] = chunk["Collection date"].apply(parse_date_to_week)

    # Process Pango lineage and Variant
    chunk["Pango lineage cut"] = chunk["Pango lineage"].str.extract(
        r"([A-Z]+(?:\.\d+){,2})", expand=True
    )
    chunk["Variant cut"] = chunk["Variant"].str.extract(r"^\w* (\w+)", expand=True)

    return chunk


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract russian metadata from GISAID metadata dump"
    )
    parser.add_argument("metadata", help="Metadata .tar.xz achive or .tsv", type=Path)
    args = parser.parse_args()

    chunks = [process_chunk(chunk) for chunk in iter_chunks(args.metadata)]
    metadata = pd.concat(chunks, ignore_index=True)
    metadata.to_csv(f"russian-metadata-{date.today()}.tsv", index=False, sep="\t")
