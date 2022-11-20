#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date
from pathlib import Path

import click
import pandas as pd

from rii.gisaid import iter_metadata


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


@click.command()
@click.argument(
    "metadata",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
def extract_russian_metadata(metadata: Path) -> None:
    """Extract russian metadata from GISAID metadata dump (.tar.xz achive or .tsv)"""

    chunks = [process_chunk(chunk) for chunk in iter_metadata(metadata)]
    df = pd.concat(chunks, ignore_index=True)
    df.to_csv(f"russian-metadata-{date.today()}.tsv", index=False, sep="\t")


if __name__ == "__main__":
    extract_russian_metadata()
