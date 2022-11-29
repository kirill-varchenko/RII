import warnings
from pathlib import Path
from typing import Literal

import altair as alt
import click
import pandas as pd

from rii.gisaid import make_query


@click.command()
@click.argument(
    "metadata",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--pango-lineage",
    "-p",
    help="Lineage filter, unix filename-like patterns allowed, can use mupliple flags",
    multiple=True,
)
@click.option(
    "--time-step",
    "-s",
    type=click.Choice(["day", "week", "month"]),
    default="week",
    show_default=True,
)
@click.option("--time-from", "-f")
@click.option("--time-to", "-t")
@click.option("--time-clip", "-c", help="Clip time to first occurence", is_flag=True)
@click.option(
    "--output",
    "-o",
    help="Output basename",
    default="variant_region_proportion",
    show_default=True,
)
@click.option(
    "--format", type=click.Choice(["svg", "png"]), default="png", show_default=True
)
@click.option("--table", help="Write pivot table", is_flag=True)
@click.option("--rii-only", type=bool, default=False, show_default=True, is_flag=True)
@click.option("--color-scheme", default="reds", show_default=True)
def plot_variant_region_proportion(
    metadata: Path,
    pango_lineage: tuple[str],
    time_step: str,
    time_from: str,
    time_to: str,
    time_clip: bool,
    output: str = "variant_region_proportion",
    format: Literal["svg", "png"] = "png",
    table: bool = False,
    rii_only: bool = False,
    color_scheme: str = "reds",
) -> None:
    metadata_df = pd.read_csv(metadata, sep="\t")

    # Filtering
    step_to_column = {
        "day": "Collection date",
        "week": "Collection week",
        "month": "Collection month",
    }
    time_column = step_to_column[time_step]
    df = metadata_df
    if time_from is not None:
        df = df[df[time_column].ge(time_from)]
    if time_to is not None:
        df = df[df[time_column].le(time_to)]
    if rii_only:
        df = df[df["RII"]]

    selected_lineage = df.query(make_query(pango_lineage=pango_lineage))

    # Counting
    sequencing_volume = (
        df.groupby(["ISO", time_column])["Accession ID"]
        .agg("count")
        .to_frame("Sequencing volume")
        .reset_index()
    )
    selected_pango_lineages = (
        selected_lineage.groupby(["ISO", time_column])["Accession ID"]
        .agg("count")
        .to_frame("Count")
        .reset_index()
    )
    merged = sequencing_volume.merge(
        selected_pango_lineages, on=["ISO", time_column], how="left"
    ).fillna(0)
    merged["Proportion"] = (
        (100 * merged["Count"] / merged["Sequencing volume"]).round().astype(int)
    )

    if time_clip:
        cutoff = merged.loc[merged["Proportion"].gt(0), time_column].min()
        merged = merged[merged[time_column].ge(cutoff)]

    # Plotting
    title = ", ".join(pango_lineage)
    warnings.simplefilter("ignore")
    chart = (
        alt.Chart(merged)
        .mark_circle(stroke="black", strokeWidth=1)
        .encode(
            x=f"{time_column}:O",
            y="ISO:N",
            size=alt.Size("Sequencing volume:Q", scale=alt.Scale(type="log")),
            color=alt.condition(
                "datum.Proportion == 0",
                alt.value("white"),
                alt.Color("Proportion:Q", scale=alt.Scale(scheme=color_scheme)),
            ),
        )
        .properties(title=title)
    )
    output_filename = f"{output}.{format}"
    chart.save(output_filename)

    if table:
        pivot = (
            merged.pivot(index="ISO", columns=time_column, values="Proportion")
            .fillna(0)
            .astype(int)
        )
        pivot.to_excel(f"{output}.xlsx")


if __name__ == "__main__":
    plot_variant_region_proportion()
