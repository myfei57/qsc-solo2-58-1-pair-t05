"""Resumable ordered execution engine for durable batch jobs.

Rules enforced here:

* steps execute strictly in plan order, one at a time;
* the RUNNING state is persisted *before* the device is touched, so a
  crash mid-command never leads to a blind re-issue;
* DONE is terminal — resume never touches it;
* a BLOCKED step halts the chain and everything behind it stays PENDING
  (explicit 待处理) until an operator retries or confirms it;
* reordering recomputes only open work; settled records are untouched.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Protocol

from portpower.batch.model import (
    PHYSICAL_CLOSED,
    PHYSICAL_OPEN,
    PHYSICAL_TRIPPED,
    BatchItem,
    BatchJob,
    JobState,
    StepRecord,
    StepState,
)
from portpower.batch.plan import plan_order
from portpower.batch.repository import BatchRepository

OPEN_PHYSICAL = frozenset({PHYSICAL_OPEN})
BLOCKING_PHYSICAL = frozenset({PHYSICAL_TRIPPED})


class BatchStateError(RuntimeError):
    """Raised when an operation is invalid for the batch's progress."""


@dataclass(frozen=True)
class StepOutcome:
    """Result of one gateway attempt.

    ``applied`` is False when the breaker was already in the target
    state (a reconciliation rather than a new command).
    """

    physical_after: str
    applied: bool = True
    note: str = ""


class StepGateway(Protocol):
    """Device-facing boundary for one batch kind.

    ``observe`` reads real equipment state and never mutates it;
    ``apply`` issues the single command for one item.
    """

    def observe(self, key: str, item: BatchItem) -> str: ...

    def apply(self, key: str, item: BatchItem) -> StepOutcome: ...


class BatchRunner:
    def __init__(
        self,
        repository: BatchRepository,
        gateways: dict[str, StepGateway],
        ledger=None,
    ) -> None:
        self._repository = repository
        self._gateways = gateways
        self._ledger = ledger
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def advance(self, job_id: str, max_steps: int | None = None) -> BatchJob:
        """Push the batch forward from its breakpoint.

        DONE steps are skipped without touching equipment; the open head
        is attempted whether it is queued (PENDING) or parked at the last
        failure (BLOCKED). The first step that cannot be settled stops the
        run, and everything behind it stays explicitly PENDING.
        """

        with self._lock_for(job_id):
            job = self._repository.load(job_id)
            self._require_active(job)
            gateway = self._gateway_for(job)
            self._recover_running(job, gateway)

            attempted = 0
            for key in job.order:
                record = job.records[key]
                if record.done:
                    continue
                if not self._prerequisites_settled(job, key):
                    break
                # The open head is attempted whether it is queued (PENDING)
                # or sitting at the breakpoint (BLOCKED). A resume after the
                # field crew cleared the jam therefore picks up exactly here;
                # observe() still refuses to dispatch into a tripped breaker.
                self._attempt(job, gateway, key, record)
                attempted += 1
                if record.done and job.all_done:
                    job.state = JobState.COMPLETED
                    self._repository.save(job)
                    self._audit(job, "batch.completed", "all steps settled")
                    break
                if record.blocked:
                    self._repository.save(job)
                    break
                if max_steps is not None and attempted >= max_steps:
                    self._repository.save(job)
                    break
                self._repository.save(job)
            else:
                if job.all_done and job.state is JobState.ACTIVE:
                    job.state = JobState.COMPLETED
                    self._repository.save(job)
                    self._audit(job, "batch.completed", "all steps settled")
            return job

    def retry(self, job_id: str, key: str) -> BatchJob:
        """Re-arm the head BLOCKED step as PENDING, then resume."""

        with self._lock_for(job_id):
            job = self._repository.load(job_id)
            self._require_active(job)
            record = self._require_record(job, key)
            if not record.blocked:
                raise BatchStateError(
                    f"{key} is {record.state.value}, only blocked steps retry"
                )
            self._require_head(job, key)
            record.append_event(StepState.PENDING, "operator retry requested")
            self._repository.save(job)
            self._audit(job, "batch.retry", key)
        return self.advance(job_id)

    def skip(self, job_id: str, key: str, note: str) -> BatchJob:
        """Mark the head BLOCKED step settled after on-site verification.

        This records an explicit operator acknowledgement; it is the only
        way past a step whose command outcome could not be confirmed.
        """

        with self._lock_for(job_id):
            job = self._repository.load(job_id)
            self._require_active(job)
            record = self._require_record(job, key)
            if not record.blocked:
                raise BatchStateError(
                    f"{key} is {record.state.value}, only blocked steps skip"
                )
            self._require_head(job, key)
            self._settle(
                job,
                record,
                note=note or "operator confirmed on site, step skipped",
            )
            if job.all_done:
                job.state = JobState.COMPLETED
            self._repository.save(job)
            self._audit(job, "batch.skip", f"{key}: {note}")
        if job.state is JobState.ACTIVE:
            return self.advance(job_id)
        return job

    def reorder(self, job_id: str) -> BatchJob:
        """Re-apply safety constraints to the open work.

        Settled records keep their finish order; only PENDING/BLOCKED
        positions may move, and no record field is modified.
        """

        with self._lock_for(job_id):
            job = self._repository.load(job_id)
            self._require_active(job)
            new_order = plan_order(job.items, job.records)
            if new_order != job.order:
                job.order = new_order
                job.plan_version += 1
                self._repository.save(job)
                self._audit(
                    job,
                    "batch.reorder",
                    f"plan v{job.plan_version}: {', '.join(new_order)}",
                )
            return job

    def update_constraints(
        self,
        job_id: str,
        requires: dict[str, list[str]] | None = None,
        priority: dict[str, int] | None = None,
    ) -> BatchJob:
        """Adjust safety constraints, then reorder the open work.

        Only the item's constraint fields change; records are read only.
        """

        with self._lock_for(job_id):
            job = self._repository.load(job_id)
            self._require_active(job)
            for key, prerequisites in (requires or {}).items():
                if key not in job.items:
                    raise BatchStateError(f"unknown item {key}")
                if job.records[key].done:
                    raise BatchStateError(
                        f"{key} is settled; its constraints are frozen"
                    )
                item = job.items[key]
                unknown = [value for value in prerequisites if value not in job.items]
                if unknown:
                    raise BatchStateError(
                        f"unknown constraint targets for {key}: {', '.join(unknown)}"
                    )
                if key in prerequisites:
                    raise BatchStateError(f"{key} cannot require itself")
                job.items[key] = BatchItem(
                    key=item.key,
                    label=item.label,
                    requires=frozenset(prerequisites),
                    priority=item.priority,
                    extra=item.extra,
                )
            for key, value in (priority or {}).items():
                if key not in job.items:
                    raise BatchStateError(f"unknown item {key}")
                if job.records[key].done:
                    raise BatchStateError(
                        f"{key} is settled; its priority is frozen"
                    )
                item = job.items[key]
                job.items[key] = BatchItem(
                    key=item.key,
                    label=item.label,
                    requires=item.requires,
                    priority=value,
                    extra=item.extra,
                )
            new_order = plan_order(job.items, job.records)
            job.order = new_order
            job.plan_version += 1
            self._repository.save(job)
            self._audit(
                job,
                "batch.constraints",
                f"plan v{job.plan_version}: {', '.join(new_order)}",
            )
            return job

    # -- internals -------------------------------------------------------

    def _attempt(self, job, gateway, key: str, record: StepRecord) -> None:
        item = job.items[key]
        try:
            observed = gateway.observe(key, item)
        except Exception as exc:  # observation must never crash the batch
            self._block(
                record,
                f"state read failed: {type(exc).__name__}: {exc}",
                physical_after="unknown",
            )
            return

        record.physical_before = observed
        if observed in OPEN_PHYSICAL:
            # Real equipment already matches the goal: reconcile instead
            # of issuing a second command. This is what makes reruns safe.
            record.physical_after = observed
            self._settle(
                job,
                record,
                note=f"already {observed} before command; reconciled, not re-issued",
            )
            self._audit(job, "batch.step.reconciled", key)
            return
        if observed in BLOCKING_PHYSICAL:
            self._block(
                record,
                record.detail
                or f"breaker {observed}; reset and clear the fault first",
                physical_after=observed,
            )
            return

        # WAL: the in-flight marker is durable before any command goes out.
        record.attempts += 1
        record.append_event(StepState.RUNNING, f"observed {observed}, dispatching")
        self._repository.save(job)

        try:
            outcome = gateway.apply(key, item)
        except Exception as exc:
            self._block(
                record,
                f"command failed: {type(exc).__name__}: {exc}",
            )
            return

        record.physical_after = outcome.physical_after
        if outcome.physical_after in OPEN_PHYSICAL:
            self._settle(
                job,
                record,
                note=outcome.note or "opened",
                applied=outcome.applied,
            )
            self._audit(job, "batch.step.done", key)
        elif outcome.physical_after in BLOCKING_PHYSICAL:
            self._block(
                record,
                "breaker tripped while opening; inspect before retry",
            )
        else:
            # Command returned but the breaker stayed shut: the stuck case.
            self._block(
                record,
                f"breaker stuck: still {outcome.physical_after} after open command",
            )

    def _recover_running(self, job: BatchJob, gateway: StepGateway) -> None:
        """Resolve steps left RUNNING by a crash between command and ack."""

        for key in job.order:
            record = job.records[key]
            if not record.running:
                continue
            try:
                observed = gateway.observe(key, job.items[key])
            except Exception as exc:
                observed = "unknown"
            record.physical_after = observed
            if observed in OPEN_PHYSICAL:
                self._settle(
                    job,
                    record,
                    note="recovered: open command completed before restart",
                )
                self._audit(job, "batch.step.recovered", key)
            elif observed == PHYSICAL_CLOSED:
                # Command clearly never took effect: safe to re-dispatch.
                record.detail = (
                    "recovered: no effect before restart, re-dispatching"
                )
                record.append_event(
                    StepState.PENDING,
                    "recovered closed, command will be retried",
                )
            else:
                # Tripped or unreadable: a blind re-issue is unsafe.
                record.detail = (
                    f"interrupted mid-command, breaker {observed}; manual check"
                )
                record.append_event(StepState.BLOCKED, record.detail)
            self._repository.save(job)

    def _settle(
        self,
        job: BatchJob,
        record: StepRecord,
        note: str,
        applied: bool = False,
    ) -> None:
        sequences = [
            settled.finish_seq
            for settled in job.records.values()
            if settled.finish_seq is not None
        ]
        record.finish_seq = (max(sequences) + 1) if sequences else 1
        record.detail = note
        record.append_event(
            StepState.DONE,
            ("applied; " if applied else "") + note,
        )

    def _block(
        self,
        record: StepRecord,
        detail: str,
        physical_after: str | None = None,
    ) -> None:
        record.detail = detail
        if physical_after is not None:
            record.physical_after = physical_after
        record.append_event(StepState.BLOCKED, detail)

    def _prerequisites_settled(self, job: BatchJob, key: str) -> bool:
        return all(
            job.records[prerequisite].done
            for prerequisite in job.items[key].requires
        )

    def _gateway_for(self, job: BatchJob) -> StepGateway:
        gateway = self._gateways.get(job.kind)
        if gateway is None:
            raise BatchStateError(f"no gateway registered for kind {job.kind}")
        return gateway

    @staticmethod
    def _require_active(job: BatchJob) -> None:
        if job.state is JobState.COMPLETED:
            raise BatchStateError(f"batch {job.job_id} already completed")

    @staticmethod
    def _require_record(job: BatchJob, key: str) -> StepRecord:
        record = job.records.get(key)
        if record is None:
            raise BatchStateError(f"unknown item {key}")
        return record

    @staticmethod
    def _require_head(job: BatchJob, key: str) -> None:
        for candidate in job.order:
            if not job.records[candidate].done:
                if candidate != key:
                    raise BatchStateError(
                        f"{candidate} must be settled before touching {key}"
                    )
                return

    def _audit(self, job: BatchJob, event: str, detail: str) -> None:
        if self._ledger is not None:
            self._ledger.append(event, f"batch {job.job_id}: {detail}")

    def _lock_for(self, job_id: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(job_id)
            if lock is None:
                lock = threading.Lock()
                self._locks[job_id] = lock
            return lock
