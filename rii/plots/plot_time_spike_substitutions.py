import fnmatch
import warnings
from functools import reduce
from operator import or_
from pathlib import Path
from typing import Literal

import altair as alt
import click
import pandas as pd


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
@click.option("--frequency-cutoff", type=float, default=0.5, show_default=True)
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
    default="spike_substitutions",
    show_default=True,
)
@click.option(
    "--format", type=click.Choice(["svg", "png"]), default="png", show_default=True
)
def plot_time_spike_substitutions(
    metadata: Path,
    pango_lineage: tuple[str],
    frequency_cutoff: float,
    time_step: str,
    time_from: str,
    time_to: str,
    output: str = "time_spike_substitutions",
    format: Literal["svg", "png"] = "png",
) -> None:
    metadata_df = pd.read_csv(
        metadata,
        sep="\t",
        usecols=[
            "Accession ID",
            "AA Substitutions",
            "Collection date",
            "Collection week",
            "Collection month",
            "Pango lineage",
        ],
    ).set_index("Accession ID")

    # Filtering
    step_to_column = {
        "day": "Collection date",
        "week": "Collection week",
        "month": "Collection month",
    }
    time_column = step_to_column[time_step]
    if time_from is not None:
        metadata_df = metadata_df[metadata_df[time_column].ge(time_from)]
    if time_to is not None:
        metadata_df = metadata_df[metadata_df[time_column].le(time_to)]
    if pango_lineage:
        pango_selector = reduce(
            or_,
            [
                metadata_df["Pango lineage"].str.fullmatch(fnmatch.translate(pattern))
                for pattern in pango_lineage
            ],
        )
        metadata_df = metadata_df[pango_selector]

    # Preparing
    spike_substitution_pattern = r"(?P<aa_sub>Spike_[A-Za-z]+\d+[A-Za-z]+)"

    spike_subs_df = (
        metadata_df["AA Substitutions"]
        .str.extractall(spike_substitution_pattern)
        .dropna()
        .reset_index()
        .drop(columns=["match"])
    )

    aa_freq = spike_subs_df["aa_sub"].value_counts().to_frame("count") / len(
        metadata_df
    )
    selected_aa_subs = list(aa_freq[aa_freq >= frequency_cutoff].dropna().index)
    meta_aa_df = metadata_df.merge(
        spike_subs_df[spike_subs_df["aa_sub"].isin(selected_aa_subs)],
        left_index=True,
        right_on="Accession ID",
    )

    aa_count_df = (
        meta_aa_df.groupby([time_column, "aa_sub"])["Accession ID"]
        .agg("count")
        .to_frame("count")
        .reset_index()
    )
    meta_count_df = (
        metadata_df.groupby(time_column).size().to_frame("total").reset_index()
    )
    meta_aa_count_df = meta_count_df.merge(aa_count_df, on=time_column)
    meta_aa_count_df["freq"] = meta_aa_count_df["count"] / meta_aa_count_df["total"]
    meta_aa_count_df["pos"] = (
        meta_aa_count_df["aa_sub"].str.extract(r"(\d+)").astype(int)
    )

    # Plot
    warnings.simplefilter("ignore")
    chart = (
        alt.Chart(meta_aa_count_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{time_column}:O", title=None),
            y=alt.Y(
                "aa_sub:N", title="Замена", sort=alt.SortField("pos", order="ascending")
            ),
            color=alt.Color(
                "freq:Q", title="Доля", scale=alt.Scale(scheme="orangered")
            ),
        )
    )
    output_filename = f"{output}.{format}"
    chart.save(output_filename)


if __name__ == "__main__":
    plot_time_spike_substitutions()
