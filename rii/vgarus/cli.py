import json
from pathlib import Path
from typing import Optional

import click
import pandas as pd
from Bio import SeqIO

import rii.vgarus.client
import rii.vgarus.package


@click.group()
def cli():
    pass


@cli.command()
@click.argument("package", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def split_package(package: Path) -> None:
    """Split single json package to fasta and metadata tsv."""

    with open(package, "r") as fi:
        input_package = json.load(fi)

    metadata, sequences = rii.vgarus.package.split_package(input_package)

    metadata.to_csv(package.with_suffix(".tsv"), sep="\t", index=False)
    SeqIO.write(sequences, package.with_suffix(".fasta"), "fasta")


@cli.command()
@click.option(
    "--metadata", "-m", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "--fasta", "-f", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
def combine_package(metadata: Path, fasta: Path) -> None:
    """Combine metadata tsv and fasta to a single json package."""

    metadata_df = pd.read_csv(
        metadata, sep="\t", dtype=rii.vgarus.package.METADATA_DTYPES
    )
    seq_records = list(SeqIO.parse(fasta, "fasta"))

    package = rii.vgarus.package.combine_package(metadata_df, seq_records)

    with open(metadata.with_suffix(".json"), "w") as fo:
        json.dump(package, fo, ensure_ascii=False, indent=2)


@cli.command()
@click.option("--username", "-u")
@click.option("--password", "-p")
@click.option(
    "--env",
    "-e",
    type=click.Path(dir_okay=False, path_type=Path),
    default=Path("~/.vgarus.env"),
    show_default=True,
)
def dictionaries(
    username: Optional[str], password: Optional[str], env: Optional[Path]
) -> None:
    """Get VGARUS dictionaries."""

    try:
        if username and password:
            vgarus_auth = rii.vgarus.client.VgarusAuth(
                username=username, password=password
            )
        elif env and env.expanduser().exists():
            vgarus_auth = rii.vgarus.client.VgarusAuth(_env_file=env)  # type: ignore
        else:
            vgarus_auth = rii.vgarus.client.VgarusAuth()  # type: ignore
    except:
        click.echo("Pass username and password or env file")
        return

    client = rii.vgarus.client.VgarusClient(vgarus_auth)
    dicts = client.get_dictionary()
    print(json.dumps(dicts, ensure_ascii=False, indent=2))


@cli.command()
@click.argument("package", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--username", "-u")
@click.option("--password", "-p")
@click.option(
    "--env",
    "-e",
    type=click.Path(dir_okay=False, path_type=Path),
    default=Path("~/.vgarus.env"),
    show_default=True,
)
@click.option("--chunk-size", "-c", type=int, default=1, show_default=True)
@click.option("--output-name", "-o")
def upload(
    package: Path,
    username: Optional[str],
    password: Optional[str],
    env: Optional[Path],
    chunk_size: int = 1,
    output_name: Optional[str] = None,
) -> None:
    """Upload json package to VGARUS."""

    try:
        if username and password:
            vgarus_auth = rii.vgarus.client.VgarusAuth(
                username=username, password=password
            )
        elif env and env.expanduser().exists():
            vgarus_auth = rii.vgarus.client.VgarusAuth(_env_file=env)  # type: ignore
        else:
            vgarus_auth = rii.vgarus.client.VgarusAuth()  # type: ignore
    except:
        click.echo("Pass username and password or env file")
        return

    client = rii.vgarus.client.VgarusClient(vgarus_auth)

    with open(package, "r") as fi:
        package_data = json.load(fi)

    result = client.upload(package_data, chunk_size)
    df = pd.DataFrame(result)
    if output_name is not None:
        result_path = (package.parent / output_name).with_suffix(".tsv")
    else:
        result_path = package.with_suffix(".tsv")
    df.to_csv(result_path, sep="\t", index=False)


if __name__ == "__main__":
    cli()
