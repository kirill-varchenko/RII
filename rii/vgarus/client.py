import logging
import math
from datetime import date
from http import HTTPStatus
from typing import Generator, Optional

import requests
import requests.auth
from pydantic import BaseSettings
from tqdm import tqdm

logger = logging.getLogger(__name__)

class VgarusAuth(BaseSettings):
    username: str
    password: str

    class Config:
        env_prefix = "VGARUS_"
        case_sensitive = False


class VgarusClient:
    UPLOAD_URL: str = "https://genome.crie.ru/api/v1/import/package"
    DICTIONARY_URL: str = "https://genome.crie.ru/api/v1/import/dictionary"

    TIMEOUT: int = 60

    def __init__(self, auth: VgarusAuth) -> None:
        self.auth = requests.auth.HTTPBasicAuth(
            username=auth.username, password=auth.password
        )
        self.today = str(date.today())

    @staticmethod
    def _iter_chunks(
        package: list[dict], chunk_size: int
    ) -> Generator[list[dict], None, None]:
        if chunk_size <= 0:
            raise ValueError(chunk_size)
        for i in range(0, len(package), chunk_size):
            yield package[i : i + chunk_size]

    def get_dictionary(self) -> dict:
        response = requests.get(
            self.DICTIONARY_URL,
            auth=self.auth,
            timeout=self.TIMEOUT,
        )
        return response.json()

    def send_chunk(self, chunk: list[dict]) -> list[dict]:
        response_data: dict = {}
        status: Optional[HTTPStatus] = None

        try:
            response = requests.post(
                self.UPLOAD_URL,
                auth=self.auth,
                json=chunk,
                timeout=self.TIMEOUT,
            )

            response_data = response.json()
            status = HTTPStatus(response.status_code)
        except:
            logger.exception("Sending chunk failed")

        if status == HTTPStatus.OK:
            vgarus_ids = response_data.get("message", [])
        else:
            logger.warning("Status not OK, response: %s", response_data)
            vgarus_ids = [None] * len(chunk)

        accession_ids = [item["sample_data"].get("gisaid_id") for item in chunk]

        if len(vgarus_ids) != len(accession_ids):
            logger.warning(
                "IDs lengths mismatch, vgarus %s, gisaid %s, response: %s",
                len(vgarus_ids),
                len(accession_ids),
                response_data,
            )

        return [
            {
                "accession_id": accession_id,
                "vgarus_id": vgarus_id,
                "submission_date": self.today,
            }
            for accession_id, vgarus_id in zip(accession_ids, vgarus_ids)
        ]

    def upload(self, package: list[dict], chunk_size: int = 1) -> list[dict]:
        chunks = math.ceil(len(package) / chunk_size)
        logger.info("Uploading %s records in %s chunks", len(package), chunks)

        results: list[dict] = []
        ok = 0
        not_ok = 0
        with tqdm(desc="Uploading", total=chunks) as progress:
            for chunk in self._iter_chunks(package, chunk_size):
                result = self.send_chunk(chunk)
                results.extend(result)
                s = sum([r["vgarus_id"] is not None and r["vgarus_id"].startswith("infl") for r in result])
                ok += s
                not_ok += len(chunk) - s
                progress.set_postfix(ok=ok, not_ok=not_ok)
                progress.update()

        return results
                