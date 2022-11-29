import io
import re
from typing import Any

import pandas as pd
from Bio import SeqIO
from Bio.SeqRecord import SeqRecord

METADATA_DTYPES = {
    "seq_area": pd.UInt8Dtype(),
    "lung_damage": pd.UInt8Dtype(),
    "issue": pd.UInt8Dtype(),
    "biomater": pd.UInt8Dtype(),
    "foreign": pd.UInt8Dtype(),
    "double_sick": pd.UInt8Dtype(),
    "vaccine": pd.UInt8Dtype(),
    "sample_pick_place": pd.StringDtype(),
    "sample_pick_date": pd.StringDtype(),
    "patient_age": pd.UInt8Dtype(),
    "patient_gender": pd.UInt8Dtype(),
    "author": pd.StringDtype(),
    "sample_name": pd.StringDtype(),
    "sample_type": pd.UInt8Dtype(),
    "gisaid_id": pd.StringDtype(),
    "tech": pd.UInt8Dtype(),
    "genom_pick_method": pd.StringDtype(),
}

RE_NORMALIZE_SAMPLE_NAME = re.compile(r"[^a-zA-Z0-9_-]")


def normalize_sample_name(sample_name: str) -> str:
    return RE_NORMALIZE_SAMPLE_NAME.sub("_", sample_name)


def drop_na(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None and not pd.isna(v)}


def split_package(
    package: list[dict[str, Any]]
) -> tuple[pd.DataFrame, list[SeqRecord]]:
    seq_records: list[SeqRecord] = []
    sample_data = []
    for item in package:
        sample_data.append(item["sample_data"])
        with io.StringIO(item["sequence"]) as fasta_io:
            seq_records.append(SeqIO.read(fasta_io, "fasta"))
    sample_df = pd.DataFrame(sample_data)
    return sample_df, seq_records


def combine_package(
    metadata: pd.DataFrame, seq_records: list[SeqRecord]
) -> list[dict[str, Any]]:
    sequencies: dict[str, str] = {}
    for seq_record in seq_records:
        seq_record.id = normalize_sample_name(seq_record.description)
        seq_record.name = ""
        seq_record.description = ""
        sequencies[seq_record.id] = seq_record.format("fasta")

    metadata["sample_name"] = metadata["sample_name"].apply(normalize_sample_name)
    rows = metadata.to_dict("records")

    package = []
    for row in rows:
        package.append(
            {"sample_data": drop_na(row), "sequence": sequencies[row["sample_name"]]}
        )

    return package
