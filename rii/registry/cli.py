from pathlib import Path
from typing import Literal

import click
import yaml

import rii.registry.etl


@click.command()
@click.argument("table", type=click.Choice(["gz", "pcr_21-22", "pcr_22-23"]))
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def extract(table: Literal["gz", "pcr_21-22", "pcr_22-23"], file: Path) -> None:
    """Extract data from registry excel file to tsv."""

    with open("rii/registry/extract.yml", "r") as fi:
        extract_schemes = yaml.load(fi, Loader=yaml.Loader)

    data = rii.registry.etl.extract(table, file, extract_schemes=extract_schemes)

    data.to_csv(f"{table}.tsv", sep="\t", index=False)


if __name__ == "__main__":
    extract()
