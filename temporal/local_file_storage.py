"""A local-directory External Storage driver, standing in for S3 in the demo.

Temporal's External Storage (experimental in temporalio 1.36) stores any
payload at or above a size threshold through a driver and writes only a
reference into history. The driver interface has store and retrieve, and
no delete: removing stored objects is the job of the store's own lifecycle
policy or its operator, and it must outlive every history that refers to
them.

Set DEMO_EXTERNAL_STORAGE_DIR on the worker and the driver to turn it on.
Use an empty directory per run so its object count belongs to that run.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import os
import re
import tempfile
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
OBJECT_KEY_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class LocalFileStorageDriver(StorageDriver):
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def name(self) -> str:
        return "local-file"

    async def store(
        self, context: StorageDriverStoreContext, payloads: Sequence[Payload]
    ) -> list[StorageDriverClaim]:
        return await asyncio.to_thread(self._store_payloads, payloads)

    async def retrieve(
        self, context: StorageDriverRetrieveContext, claims: Sequence[StorageDriverClaim]
    ) -> list[Payload]:
        return await asyncio.to_thread(self._retrieve_payloads, claims)

    def _store_payloads(self, payloads: Sequence[Payload]) -> list[StorageDriverClaim]:
        self._directory.mkdir(parents=True, exist_ok=True)
        claims = []
        for payload in payloads:
            data = payload.SerializeToString()
            # Content-addressed like the official S3 driver.
            key = hashlib.sha256(data).hexdigest()
            object_path = self._directory / key
            if not object_path.exists():
                self._write_atomically(object_path, data)
            claims.append(StorageDriverClaim({"key": key}))
        return claims

    def _write_atomically(self, object_path: Path, data: bytes) -> None:
        # Readers never see a partly written object: write aside, then rename.
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self._directory, prefix=".incoming-"
        )
        try:
            with os.fdopen(descriptor, "wb") as temporary_file:
                temporary_file.write(data)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_name, object_path)
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise

    def _retrieve_payloads(self, claims: Sequence[StorageDriverClaim]) -> list[Payload]:
        payloads = []
        for claim in claims:
            key = claim.claim_data.get("key", "")
            if not OBJECT_KEY_PATTERN.match(key):
                raise ValueError(f"invalid external storage key {key!r}")
            data = (self._directory / key).read_bytes()
            if hashlib.sha256(data).hexdigest() != key:
                raise ValueError(f"external storage object {key} does not match its key")
            payload = Payload()
            payload.ParseFromString(data)
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
