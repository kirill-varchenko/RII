import warnings
from pathlib import Path
from typing import Literal

import altair as alt
import click
import pandas as pd

from rii.gisaid import make_query
from rii.helpers import count_frequency


@click.command()
@click.argument(
    "metadata",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option("--frequency-cutoff", type=float, default=0.01, show_default=True)
@click.option(
    "--time-step",
    "-s",
    type=click.Choice(["day", "week", "month"]),
    default="week",
    show_default=True,
)
@click.option("--time-from", "-f")
@click.option("--time-to", "-t")
@click.option(
    "--output",
    "-o",
    help="Output basename",
    default="pango_bar",
    show_default=True,
)
@click.option(
    "--format", type=click.Choice(["svg", "png"]), default="png", show_default=True
)
@click.option("--table", help="Write pivot table", is_flag=True)
@click.option("--color-scheme", default="reds", show_default=True)
def plot_pango_bar(
    metadata: Path,
    frequency_cutoff: float,
    time_step: str,
    time_from: str,
    time_to: str,
    output: str = "pango_bar",
    format: Literal["svg", "png"] = "png",
    table: bool = False,
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

    # Prepare data
    frequencies = count_frequency(df, column="Pango lineage combo")
    selected_lineages = frequencies.loc[
        frequencies["Pango lineage combo Frequency"].ge(frequency_cutoff),
        "Pango lineage combo",
    ].to_list()

    df.loc[
        df["Pango lineage combo"].isin(selected_lineages), "Pango lineage prepared"
    ] = df["Pango lineage combo"]
    df["Pango lineage prepared"].fillna("Другое", inplace=True)

    df.to_csv("data.csv", index=False)

    # Plotting
    alt.data_transformers.disable_max_rows()
    warnings.simplefilter("ignore")
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{time_column}:O", title=None),
            y=alt.Y("count()", stack="normalize", title="Доля"),
            color=alt.Color("Pango lineage prepared:N", title="Линия"),
        )
    )

    output_filename = f"{output}.{format}"
    chart.save(output_filename)

    if table:
        stepped_frequencies = count_frequency(
            df, column="Pango lineage prepared", groupby=[time_column]
        )
        pivot = stepped_frequencies.pivot(
            index=time_column,
            columns="Pango lineage prepared",
            values=f"{time_column} Pango lineage prepared Frequency",
        ).fillna(0)
        other_counts = (
            df.loc[df["Pango lineage prepared"].eq("Другое"), "Pango lineage combo"]
            .value_counts()
            .to_frame()
        )

        with pd.ExcelWriter(f"{output}.xlsx") as writer:
            pivot.to_excel(writer, sheet_name="Pivot")
            other_counts.to_excel(writer, sheet_name="Other")


if __name__ == "__main__":
    plot_pango_bar()
