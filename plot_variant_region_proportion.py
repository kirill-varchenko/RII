import warnings
from pathlib import Path

import altair as alt
import click
import pandas as pd


@click.command()
@click.argument(
    "metadata",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option("--pango-lineage", "-p", multiple=True)
@click.option(
    "--time-step",
    "-s",
    type=click.Choice(["day", "week", "month"]),
    default="week",
    show_default=True,
)
@click.option("--time-from", "-f")
@click.option("--time-to", "-t")
@click.option("--output", "-o")
@click.option("--rii-only", type=bool, default=False, show_default=True, is_flag=True)
def plot_variant_region_proportion(
    metadata: Path,
    pango_lineage: list[str],
    time_step: str,
    time_from: str,
    time_to: str,
    rii_only: bool,
    output: str,
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

    # Counting
    sequencing_volume = (
        df.groupby(["ISO", time_column])["Accession ID"]
        .agg("count")
        .to_frame("Sequencing volume")
        .reset_index()
    )
    selected_pango_lineages = (
        df[df["Pango lineage"].isin(pango_lineage)]
        .groupby(["ISO", time_column])["Accession ID"]
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

    # Plotting
    title = ", ".join(pango_lineage)
    warnings.simplefilter("ignore")
    chart = (
        alt.Chart(merged)  # type: ignore
        .mark_circle()
        .encode(
            x=f"{time_column}:O",
            y="ISO:N",
            size=alt.Size("Sequencing volume:Q", scale=alt.Scale(type="log")),  # type: ignore
            color=alt.Color("Proportion:Q", scale=alt.Scale(scheme="inferno")),  # type: ignore
        )
        .properties(title=title)
    )
    output_filename = output if output else f"{title}.png".replace(" ", "")
    chart.save(output_filename)


if __name__ == "__main__":
    plot_variant_region_proportion()
