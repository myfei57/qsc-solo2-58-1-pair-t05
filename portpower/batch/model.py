"""Durable state machine for ordered batch operations.

A batch has two halves that are deliberately kept independent:

* the **plan** — an ordered list of stable item keys plus the safety
  constraints between them. The plan may be reordered at any time;
* the **records** — one :class:`StepRecord` per stable key, append-only.
  A record only ever transitions forward and is never rewritten when the
  plan order changes.

Because records are keyed by stable item identity (never by ordinal
position), reordering unfinished steps cannot touch a result that has
already landed.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum


class StepState(str, Enum):
    """Lifecycle of one batch item."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    BLOCKED = "blocked"

    @property
    def settled(self) -> bool:
        """A settled record must never be executed or rewritten."""

        return self is StepState.DONE

    @property
    def open(self) -> bool:
        """Open work that a resume still has to deal with."""

        return self is not StepState.DONE


class JobState(str, Enum):
    """Lifecycle of the whole batch."""

    ACTIVE = "active"
    COMPLETED = "completed"


# Physical states reported by the device gateway. These describe the real
# equipment, not the bookkeeping in the batch records.
PHYSICAL_OPEN = "open"
PHYSICAL_CLOSED = "closed"
PHYSICAL_TRIPPED = "tripped"
PHYSICAL_UNKNOWN = "unknown"


def timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class BatchItem:
    """One stable unit of work.

    ``key`` is the identity used by records and constraints and must stay
    constant across replanning. ``label`` is human-facing (berth code,
    breaker id ...). ``requires`` lists the keys that must be settled
    before this item may execute; ``priority`` orders otherwise equal
    items (lower first).
    """

    key: str
    label: str
    requires: frozenset[str] = frozenset()
    priority: int = 0
    extra: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "label": self.label,
            "requires": sorted(self.requires),
            "priority": self.priority,
            "extra": dict(self.extra),
        }

    @classmethod
    def from_dict(cls, payload: object) -> "BatchItem | None":
        if not isinstance(payload, dict):
            return None
        key = payload.get("key")
        label = payload.get("label")
        if not isinstance(key, str) or not key:
            return None
        if not isinstance(label, str):
            return None
        requires_raw = payload.get("requires", [])
        if not isinstance(requires_raw, list) or not all(
            isinstance(value, str) for value in requires_raw
        ):
            return None
        priority = payload.get("priority", 0)
        if isinstance(priority, bool) or not isinstance(priority, int):
            return None
        extra = payload.get("extra", {})
        if not isinstance(extra, dict):
            return None
        return cls(
            key=key,
            label=label,
            requires=frozenset(requires_raw),
            priority=priority,
            extra=dict(extra),
        )


@dataclass(frozen=True)
class StepEvent:
    """One immutable fact appended to a step's history."""

    at: str
    state: str
    note: str = ""

    def as_dict(self) -> dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: object) -> "StepEvent | None":
        if not isinstance(payload, dict):
            return None
        at = payload.get("at")
        state = payload.get("state")
        note = payload.get("note", "")
        if not isinstance(at, str) or not isinstance(state, str):
            return None
        if not isinstance(note, str):
            return None
        return cls(at=at, state=state, note=note)


@dataclass
class StepRecord:
    """Append-only progress for one stable key."""

    key: str
    state: StepState = StepState.PENDING
    attempts: int = 0
    finish_seq: int | None = None
    detail: str = ""
    physical_before: str = ""
    physical_after: str = ""
    updated_at: str = ""
    history: list[StepEvent] = field(default_factory=list)

    @property
    def done(self) -> bool:
        return self.state is StepState.DONE

    @property
    def blocked(self) -> bool:
        return self.state is StepState.BLOCKED

    @property
    def running(self) -> bool:
        return self.state is StepState.RUNNING

    @property
    def pending(self) -> bool:
        return self.state is StepState.PENDING

    def append_event(self, state: StepState, note: str = "") -> None:
        self.state = state
        self.updated_at = timestamp()
        self.history.append(
            StepEvent(at=self.updated_at, state=state.value, note=note)
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "state": self.state.value,
            "attempts": self.attempts,
            "finish_seq": self.finish_seq,
            "detail": self.detail,
            "physical_before": self.physical_before,
            "physical_after": self.physical_after,
            "updated_at": self.updated_at,
            "history": [event.as_dict() for event in self.history],
        }

    @classmethod
    def from_dict(cls, payload: object) -> "StepRecord | None":
        if not isinstance(payload, dict):
            return None
        key = payload.get("key")
        if not isinstance(key, str) or not key:
            return None
        state_raw = payload.get("state", StepState.PENDING.value)
        if not isinstance(state_raw, str):
            return None
        try:
            state = StepState(state_raw)
        except ValueError:
            return None
        attempts = payload.get("attempts", 0)
        finish_seq = payload.get("finish_seq")
        detail = payload.get("detail", "")
        before = payload.get("physical_before", "")
        after = payload.get("physical_after", "")
        updated_at = payload.get("updated_at", "")
        history_raw = payload.get("history", [])
        if isinstance(attempts, bool) or not isinstance(attempts, int):
            return None
        if finish_seq is not None and (
            isinstance(finish_seq, bool) or not isinstance(finish_seq, int)
        ):
            return None
        if not all(
            isinstance(value, str)
            for value in (detail, before, after, updated_at)
        ):
            return None
        if not isinstance(history_raw, list):
            return None
        history = [
            event
            for event in (StepEvent.from_dict(item) for item in history_raw)
            if event is not None
        ]
        return cls(
            key=key,
            state=state,
            attempts=attempts,
            finish_seq=finish_seq,
            detail=detail,
            physical_before=before,
            physical_after=after,
            updated_at=updated_at,
            history=history,
        )


@dataclass
class BatchJob:
    """The persisted batch: plan plus append-only records."""

    job_id: str
    kind: str
    order: list[str]
    items: dict[str, BatchItem]
    records: dict[str, StepRecord]
    state: JobState = JobState.ACTIVE
    plan_version: int = 1
    created_at: str = field(default_factory=timestamp)
    updated_at: str = field(default_factory=timestamp)

    def record(self, key: str) -> StepRecord:
        return self.records[key]

    def ordered_records(self) -> list[StepRecord]:
        return [self.records[key] for key in self.order]

    @property
    def all_done(self) -> bool:
        return all(record.done for record in self.records.values())

    def as_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "kind": self.kind,
            "state": self.state.value,
            "plan_version": self.plan_version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "order": list(self.order),
            "items": {
                key: self.items[key].as_dict() for key in self.order
            },
            "records": {
                key: self.records[key].as_dict() for key in self.order
            },
        }

    @classmethod
    def from_dict(cls, payload: object) -> "BatchJob | None":
        if not isinstance(payload, dict):
            return None
        job_id = payload.get("job_id")
        kind = payload.get("kind")
        if not isinstance(job_id, str) or not job_id:
            return None
        if not isinstance(kind, str) or not kind:
            return None
        state_raw = payload.get("state", JobState.ACTIVE.value)
        version = payload.get("plan_version", 1)
        created_at = payload.get("created_at", "")
        updated_at = payload.get("updated_at", "")
        order = payload.get("order", [])
        items_raw = payload.get("items", {})
        records_raw = payload.get("records", {})
        if not isinstance(state_raw, str):
            return None
        if isinstance(version, bool) or not isinstance(version, int):
            return None
        if not all(isinstance(value, str) for value in (created_at, updated_at)):
            return None
        if not isinstance(order, list) or not all(
            isinstance(value, str) for value in order
        ):
            return None
        if not isinstance(items_raw, dict) or not isinstance(records_raw, dict):
            return None
        items: dict[str, BatchItem] = {}
        for key, raw in items_raw.items():
            item = BatchItem.from_dict(raw)
            if item is None or item.key != key:
                return None
            items[key] = item
        records: dict[str, StepRecord] = {}
        for key, raw in records_raw.items():
            record = StepRecord.from_dict(raw)
            if record is None or record.key != key:
                return None
            records[key] = record
        if set(order) != set(items) or set(order) != set(records):
            return None
        try:
            state = JobState(state_raw)
        except ValueError:
            return None
        return cls(
            job_id=job_id,
            kind=kind,
            order=list(order),
            items=items,
            records=records,
            state=state,
            plan_version=version,
            created_at=created_at,
            updated_at=updated_at,
        )
