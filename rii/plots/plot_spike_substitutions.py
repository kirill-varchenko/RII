import fnmatch
import warnings
from pathlib import Path
from typing import Literal

import altair as alt
import click
import pandas as pd

from rii.gisaid import aa_substitution_pattern


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
    "--output",
    "-o",
    help="Output basename",
    default="spike_substitutions",
    show_default=True,
)
@click.option(
    "--format", type=click.Choice(["svg", "png"]), default="png", show_default=True
)
def plot_spike_substitutions(
    metadata: Path,
    pango_lineage: tuple[str],
    output: str = "spike_substitutions",
    format: Literal["svg", "png"] = "png",
) -> None:
    metadata_df = pd.read_csv(
        metadata,
        sep="\t",
        usecols=["Accession ID", "AA Substitutions", "Pango lineage"],
    ).set_index("Accession ID")

    # Preparing

    lines_dfs = []
    for line in pango_lineage:
        line_extract_df = metadata_df[
            metadata_df["Pango lineage"].str.fullmatch(fnmatch.translate(line))
        ]

        aa_subs_df = (
            line_extract_df["AA Substitutions"]
            .str.extractall(aa_substitution_pattern)
            .dropna()
        )
        aa_subs_df["pos"] = aa_subs_df["pos"].astype("Int32")
        spike_subs_df = (
            aa_subs_df.query("gene == 'Spike'")
            .reset_index()
            .drop(columns=["match", "gene"])
        )

        total_recs = len(line_extract_df)
        subs_count_df = (
            spike_subs_df.groupby(["pos", "seq"])["Accession ID"]
            .agg("count")
            .to_frame("count")
            .reset_index()
        )
        subs_count_df["proportion"] = subs_count_df["count"] / total_recs
        subs_count_df["line"] = line
        lines_dfs.append(subs_count_df)

    data = pd.concat(lines_dfs, ignore_index=True)

    warnings.simplefilter("ignore")
    chart = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            row=alt.Row("line:N", title=None),
            x=alt.X("pos:O", title="Положение"),
            y=alt.Y("proportion", title="Доля", scale=alt.Scale(domain=[0, 1])),
            color=alt.Color("seq:N", title=None, scale=alt.Scale(scheme="tableau20")),
        )
    )
    output_filename = f"{output}.{format}"
    chart.save(output_filename)


if __name__ == "__main__":
    plot_spike_substitutions()
