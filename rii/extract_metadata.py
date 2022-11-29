#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date
from pathlib import Path
from typing import Literal, Optional

import click
import pandas as pd
from tqdm import tqdm

from rii.gisaid import iter_metadata, make_query


def parse_date_to_week(str_date: str) -> str:
    try:
        return "{0.year}-W{0.week:02}".format(
            date.fromisoformat(str_date).isocalendar()
        )
    except:
        return ""


def enrich_df(df: pd.DataFrame) -> pd.DataFrame:
    # Extract ISO
    df["ISO"] = df["Virus name"].str.extract(r"Russia/([A-Z]{1,3})-", expand=True)
    df.loc[df["Location"].str.contains("Crimea"), "ISO"] = "Crimea"

    # Set RII flag
    df["RII"] = df["Virus name"].str.contains("-RII-")

    # Process Collection date
    df["Collection month"] = df["Collection date"].str.extract(
        r"(\d{4}-\d{2})", expand=True
    )
    df["Collection week"] = df["Collection date"].apply(parse_date_to_week)

    # Process Pango lineage and Variant
    df["Pango lineage cut"] = df["Pango lineage"].str.extract(
        r"([A-Z]+(?:\.\d+){,2})", expand=True
    )
    df["Variant cut"] = df["Variant"].str.extract(r"^\w* (\w+)", expand=True)

    return df


@click.command()
@click.argument(
    "metadata",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option("--location", multiple=True, help="Substring to filter by Location field")
@click.option(
    "--pango-lineage",
    help="Lineage filter, unix filename-like patterns allowed",
    multiple=True,
)
@click.option("--output", "-o", help="Output basename")
@click.option("--enrich", "-e", is_flag=True, help="Add computed columns")
@click.option("--compress", "-c", type=click.Choice(["gz", "xz"]))
def extract_metadata(
    metadata: Path,
    location: tuple[str],
    pango_lineage: tuple[str],
    output: Optional[str],
    enrich: bool,
    compress: Optional[Literal["gz", "xz"]],
) -> None:
    """Extract, filter and enrich metadata from GISAID metadata dump (.tar.xz achive or .tsv)."""

    query = make_query(location=location, pango_lineage=pango_lineage)

    output_path_items = []
    if output:
        output_path_items.append(output)
    else:
        output_path_items.append(f"metadata-{date.today()}")
    output_path_items.append(".tsv")
    if compress:
        output_path_items.append(f".{compress}")
    output_path = Path("".join(output_path_items))
    output_path.unlink(missing_ok=True)

    filtered_count = 0
    with tqdm(desc="Processing") as progress:
        for i, chunk in enumerate(iter_metadata(metadata)):
            if enrich:
                processed_df = enrich_df(chunk)
            else:
                processed_df = chunk
            if query:
                processed_df = processed_df.query(query)

            filtered_count += len(processed_df)
            processed_df.to_csv(
                output_path, sep="\t", index=False, mode="a", header=(i == 0)
            )
            progress.set_postfix(filtered=filtered_count)
            progress.update(len(chunk))


if __name__ == "__main__":
    extract_metadata()
