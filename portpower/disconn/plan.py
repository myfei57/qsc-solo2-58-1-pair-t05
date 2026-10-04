"""Persistent multi-berth departure batches with resumable ordered execution.

A batch splits into two halves:

* ``settled`` — an append-only list of :class:`StepRecord`, one per attempt,
  written the moment a step opens or fails. Records are never rewritten or
  reordered, so the settled history is the authoritative progress log.
* ``queue`` — the berths still waiting, in execution order. Reordering a
  batch rewrites only this list, which is why adjusting the sequence can
  never touch what already settled.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from portpower.store.kvstore import RecordStore
from portpower.store.query import keys_with_prefix

PREFIX = "disconnect.batch."


class StepStatus(str, Enum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"


class BatchMissing(LookupError):
    """Raised when a departure batch id is unknown."""


class OrderMismatch(ValueError):
    """Raised when a reorder is not a permutation of the pending queue."""


@dataclass(frozen=True)
class PlannedStep:
    """What one berth is supposed to do, fixed when the batch is created."""

    berth: str
    ship: str
    final_value: float

    def as_dict(self) -> dict[str, object]:
        return {
            "berth": self.berth,
            "ship": self.ship,
            "final_value": self.final_value,
        }

    @classmethod
    def from_dict(cls, payload: object) -> "PlannedStep | None":
        if not isinstance(payload, dict):
            return None
        berth = payload.get("berth")
        ship = payload.get("ship")
        final_value = payload.get("final_value")
        if not isinstance(berth, str) or not berth:
            return None
        if not isinstance(ship, str) or not ship:
            return None
        if isinstance(final_value, bool) or not isinstance(final_value, (int, float)):
            return None
        return cls(berth=berth, ship=ship, final_value=float(final_value))


@dataclass(frozen=True)
class StepRecord:
    """One settled attempt; appended when a step finishes or fails."""

    berth: str
    status: StepStatus
    final_value: float | None
    detail: str
    seq: int
    at: str

    def as_dict(self) -> dict[str, object]:
        return {
            "berth": self.berth,
            "status": self.status.value,
            "final_value": self.final_value,
            "detail": self.detail,
            "seq": self.seq,
            "at": self.at,
        }

    @classmethod
    def from_dict(cls, payload: object) -> "StepRecord | None":
        if not isinstance(payload, dict):
            return None
        berth = payload.get("berth")
        detail = payload.get("detail")
        seq = payload.get("seq")
        at = payload.get("at")
        final_value = payload.get("final_value")
        if not isinstance(berth, str) or not berth:
            return None
        try:
            status = StepStatus(payload.get("status"))
        except ValueError:
            return None
        if status is StepStatus.PENDING:
            return None
        if not isinstance(detail, str) or not isinstance(at, str):
            return None
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            return None
        if final_value is not None and (
            isinstance(final_value, bool) or not isinstance(final_value, (int, float))
        ):
            return None
        return cls(
            berth=berth,
            status=status,
            final_value=None if final_value is None else float(final_value),
            detail=detail,
            seq=seq,
            at=at,
        )


class DepartureBatch:
    """Ordered plan plus the append-only record of what already happened."""

    def __init__(
        self,
        batch_id: str,
        created_at: str,
        steps: list[PlannedStep],
        queue: list[str],
        settled: list[StepRecord],
    ) -> None:
        self._id = batch_id
        self._created_at = created_at
        self._steps = list(steps)
        self._by_berth = {step.berth: step for step in self._steps}
        self._queue = list(queue)
        self._settled = list(settled)

    @classmethod
    def create(cls, steps: list[PlannedStep]) -> "DepartureBatch":
        return cls(
            batch_id=uuid4().hex,
            created_at=_now(),
            steps=steps,
            queue=[step.berth for step in steps],
            settled=[],
        )

    @property
    def batch_id(self) -> str:
        return self._id

    @property
    def created_at(self) -> str:
        return self._created_at

    @property
    def state(self) -> str:
        if not self._queue:
            return "complete"
        if not self._settled:
            return "pending"
        return "interrupted"

    def next_step(self) -> PlannedStep | None:
        """The step at the head of the queue, i.e. the resume breakpoint."""

        if not self._queue:
            return None
        return self._by_berth[self._queue[0]]

    def record_done(self, berth: str, final_value: float, detail: str) -> None:
        self._require_head(berth)
        self._queue.pop(0)
        self._settled.append(
            StepRecord(
                berth=berth,
                status=StepStatus.DONE,
                final_value=float(final_value),
                detail=detail,
                seq=len(self._settled) + 1,
                at=_now(),
            )
        )

    def record_failure(self, berth: str, detail: str) -> None:
        """Log the failed attempt; the berth stays queued so resume retries it."""

        self._require_head(berth)
        self._settled.append(
            StepRecord(
                berth=berth,
                status=StepStatus.FAILED,
                final_value=None,
                detail=detail,
                seq=len(self._settled) + 1,
                at=_now(),
            )
        )

    def reorder(self, order: list[str]) -> None:
        """Rewrite the pending queue; settled records are out of reach here."""

        if sorted(order) != sorted(self._queue):
            raise OrderMismatch(
                f"order {order!r} must be a permutation of the pending queue "
                f"{self._queue!r}; settled steps cannot be reordered"
            )
        self._queue = list(order)

    def view(self) -> dict[str, object]:
        latest: dict[str, StepRecord] = {}
        for record in self._settled:
            latest[record.berth] = record
        counts = {StepStatus.DONE.value: 0, StepStatus.FAILED.value: 0, StepStatus.PENDING.value: 0}
        steps: list[dict[str, object]] = []
        for step in self._steps:
            record = latest.get(step.berth)
            if step.berth in self._queue:
                failed = record is not None and record.status is StepStatus.FAILED
                status = StepStatus.FAILED if failed else StepStatus.PENDING
            else:
                status = StepStatus.DONE
            counts[status.value] += 1
            steps.append(
                {
                    "berth": step.berth,
                    "ship": step.ship,
                    "status": status.value,
                    "final_value": (
                        record.final_value
                        if record is not None and record.final_value is not None
                        else step.final_value
                    ),
                    "seq": record.seq if record is not None else None,
                    "at": record.at if record is not None else None,
                    "detail": record.detail if record is not None else "",
                }
            )
        return {
            "id": self._id,
            "created_at": self._created_at,
            "state": self.state,
            "next": self._queue[0] if self._queue else None,
            "queue": list(self._queue),
            "counts": {"total": len(self._steps), **counts},
            "steps": steps,
            "settled": [record.as_dict() for record in self._settled],
        }

    def summary(self) -> dict[str, object]:
        view = self.view()
        return {
            "id": view["id"],
            "created_at": view["created_at"],
            "state": view["state"],
            "counts": view["counts"],
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self._id,
            "created_at": self._created_at,
            "steps": [step.as_dict() for step in self._steps],
            "queue": list(self._queue),
            "settled": [record.as_dict() for record in self._settled],
        }

    @classmethod
    def from_dict(cls, payload: object) -> "DepartureBatch | None":
        if not isinstance(payload, dict):
            return None
        batch_id = payload.get("id")
        created_at = payload.get("created_at")
        raw_steps = payload.get("steps")
        raw_queue = payload.get("queue")
        raw_settled = payload.get("settled")
        if not isinstance(batch_id, str) or not batch_id:
            return None
        if not isinstance(created_at, str) or not created_at:
            return None
        if not isinstance(raw_steps, list) or not raw_steps:
            return None
        steps: list[PlannedStep] = []
        for item in raw_steps:
            step = PlannedStep.from_dict(item)
            if step is None:
                return None
            steps.append(step)
        berths = [step.berth for step in steps]
        if len(set(berths)) != len(berths):
            return None
        if not isinstance(raw_queue, list) or not isinstance(raw_settled, list):
            return None
        if len(set(raw_queue)) != len(raw_queue):
            return None
        if any(berth not in berths for berth in raw_queue):
            return None
        settled: list[StepRecord] = []
        for item in raw_settled:
            record = StepRecord.from_dict(item)
            if record is None or record.berth not in berths:
                return None
            settled.append(record)
        return cls(
            batch_id=batch_id,
            created_at=created_at,
            steps=steps,
            queue=list(raw_queue),
            settled=settled,
        )

    def _require_head(self, berth: str) -> None:
        if not self._queue or self._queue[0] != berth:
            raise RuntimeError(f"step {berth} is not at the head of the pending queue")


class BatchStore:
    """Durable store for departure batches; serializes execution and reorder."""

    def __init__(self, store: RecordStore) -> None:
        self._store = store
        self._lock = threading.RLock()

    def locked(self) -> threading.RLock:
        """One batch runs at a time, like one operator at the console."""

        return self._lock

    def save(self, batch: DepartureBatch) -> None:
        self._store.put_json(PREFIX + batch.batch_id, batch.to_dict())

    def load(self, batch_id: str) -> DepartureBatch:
        batch = DepartureBatch.from_dict(self._store.get_json(PREFIX + batch_id))
        if batch is None:
            raise BatchMissing(f"unknown departure batch {batch_id}")
        return batch

    def list(self) -> list[DepartureBatch]:
        batches: list[DepartureBatch] = []
        for key in keys_with_prefix(self._store, PREFIX):
            batch = DepartureBatch.from_dict(self._store.get_json(key))
            if batch is not None:
                batches.append(batch)
        batches.sort(key=lambda batch: batch.created_at)
        return batches


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
