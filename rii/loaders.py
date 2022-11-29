import tarfile
from pathlib import Path
from typing import Generator, Optional

import pandas as pd


def iter_chunks_from_tar_or_csv(
    file: Path, tar_member: Optional[str] = None, **kwargs
) -> Generator[pd.DataFrame, None, None]:
    if ".tar" in file.suffixes:
        if tar_member is None:
            raise ValueError(tar_member)
        with tarfile.open(file) as tar:
            metadata_file = tar.extractfile(tar_member)
            assert metadata_file is not None
            yield from pd.read_csv(metadata_file, iterator=True, **kwargs)
    elif ".tsv" in file.suffixes or ".csv" in file.suffixes:
        yield from pd.read_csv(file, iterator=True, **kwargs)
    else:
        raise ValueError(file.suffixes)
