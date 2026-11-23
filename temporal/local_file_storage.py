"""A local-directory External Storage driver, standing in for S3 in the demo.

Temporal's External Storage (experimental in temporalio 1.36) stores any
payload at or above a size threshold through a driver and writes only a
reference into history. The driver interface has store and retrieve, and
no delete.

Set DEMO_EXTERNAL_STORAGE_DIR on the worker and the driver to turn it on.
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
from collections.abc import Sequence
from pathlib import Path

from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    DataConverter,
    ExternalStorage,
    StorageDriver,
    StorageDriverClaim,
    StorageDriverRetrieveContext,
    StorageDriverStoreContext,
)

EXTERNAL_STORAGE_DIR_VARIABLE = "DEMO_EXTERNAL_STORAGE_DIR"


class LocalFileStorageDriver(StorageDriver):
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def name(self) -> str:
        return "local-file"

    async def store(
        self, context: StorageDriverStoreContext, payloads: Sequence[Payload]
    ) -> list[StorageDriverClaim]:
        self._directory.mkdir(parents=True, exist_ok=True)
        claims = []
        for payload in payloads:
            data = payload.SerializeToString()
            # Content-addressed like the official S3 driver.
            key = hashlib.sha256(data).hexdigest()
            (self._directory / key).write_bytes(data)
            claims.append(StorageDriverClaim({"key": key}))
        return claims

    async def retrieve(
        self, context: StorageDriverRetrieveContext, claims: Sequence[StorageDriverClaim]
    ) -> list[Payload]:
        payloads = []
        for claim in claims:
            payload = Payload()
            payload.ParseFromString(
                (self._directory / claim.claim_data["key"]).read_bytes()
            )
            payloads.append(payload)
        return payloads


def build_data_converter() -> DataConverter:
    """Returns the default converter, with External Storage when configured."""
    directory = os.environ.get(EXTERNAL_STORAGE_DIR_VARIABLE, "")
    if not directory:
        return DataConverter.default
    return dataclasses.replace(
        DataConverter.default,
        external_storage=ExternalStorage(
            drivers=[LocalFileStorageDriver(Path(directory))]
        ),
    )
