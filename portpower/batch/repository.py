"""Persistence for durable batch jobs, backed by the JSON RecordStore."""

from __future__ import annotations

import uuid

from portpower.batch.model import (
    BatchItem,
    BatchJob,
    StepRecord,
    timestamp,
)
from portpower.batch.plan import topological_order
from portpower.store.kvstore import RecordStore

KEY_PREFIX = "batch.job."
INDEX_KEY = "batch.index"


class BatchMissing(LookupError):
    """Raised when a batch id does not exist."""


class BatchRepository:
    def __init__(self, store: RecordStore) -> None:
        self._store = store

    @staticmethod
    def job_key(job_id: str) -> str:
        return KEY_PREFIX + job_id

    def create(
        self,
        kind: str,
        items: list[BatchItem],
    ) -> BatchJob:
        if not items:
            raise ValueError("batch needs at least one item")
        keys = [item.key for item in items]
        if len(set(keys)) != len(keys):
            raise ValueError("batch item keys must be unique")

        item_map = {item.key: item for item in items}
        order = topological_order(item_map, keys)
        job_id = uuid.uuid4().hex
        now = timestamp()
        job = BatchJob(
            job_id=job_id,
            kind=kind,
            order=order,
            items=item_map,
            records={
                key: StepRecord(key=key, updated_at=now) for key in order
            },
            created_at=now,
            updated_at=now,
        )
        self._put(job)
        self._index(lambda ids: ids.append(job_id))
        return job

    def load(self, job_id: str) -> BatchJob:
        document = self._store.get_json(self.job_key(job_id))
        job = BatchJob.from_dict(document)
        if job is None:
            raise BatchMissing(f"batch {job_id} not found")
        return job

    def save(self, job: BatchJob) -> None:
        job.updated_at = timestamp()
        self._put(job)

    def list_ids(self) -> list[str]:
        document = self._store.get_json(INDEX_KEY)
        if not isinstance(document, list) or not all(
            isinstance(value, str) for value in document
        ):
            return []
        return list(document)

    def _put(self, job: BatchJob) -> None:
        self._store.put_json(self.job_key(job.job_id), job.as_dict())

    def _index(self, mutate) -> None:
        def update(document: object) -> tuple[object, None]:
            ids = list(document) if isinstance(document, list) else []
            mutate(ids)
            return ids, None

        self._store.update_json(INDEX_KEY, update)
