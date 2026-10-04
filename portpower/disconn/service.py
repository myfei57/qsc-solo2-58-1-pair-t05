"""Batch service: creates disconnect batches and renders job views."""

from __future__ import annotations

from portpower.batch.model import BatchItem, BatchJob
from portpower.batch.repository import BatchRepository
from portpower.batch.runner import BatchRunner
from portpower.disconn.gateway import DisconnectGateway, DisconnectTarget
from portpower.disconn.pool import BreakerPool


class BatchService:
    """Application facade over the repository and the registered runners."""

    def __init__(
        self,
        repository: BatchRepository,
        runner: BatchRunner,
        pool: BreakerPool,
    ) -> None:
        self.repository = repository
        self.runner = runner
        self.pool = pool

    @classmethod
    def build(cls, store, pool: BreakerPool, binder=None, ledger=None) -> "BatchService":
        repository = BatchRepository(store)
        gateway = DisconnectGateway(pool, binder)
        runner = BatchRunner(repository, {DisconnectGateway.kind: gateway}, ledger)
        return cls(repository, runner, pool)

    def create_disconnect(
        self,
        targets: list[DisconnectTarget],
    ) -> BatchJob:
        if not targets:
            raise ValueError("at least one target is required")
        items = [
            BatchItem(
                key=target.key,
                label=target.label,
                requires=target.requires,
                priority=target.priority,
                extra={"final_value": target.final_value},
            )
            for target in targets
        ]
        return self.repository.create(DisconnectGateway.kind, items)


def job_view(job: BatchJob) -> dict[str, object]:
    """Stable JSON view: plan first, per-step progress second."""

    steps = []
    for index, key in enumerate(job.order):
        record = job.records[key]
        item = job.items[key]
        steps.append(
            {
                "index": index,
                "key": key,
                "label": item.label,
                "state": record.state.value,
                "requires": sorted(item.requires),
                "priority": item.priority,
                "attempts": record.attempts,
                "finish_seq": record.finish_seq,
                "physical_before": record.physical_before,
                "physical_after": record.physical_after,
                "detail": record.detail,
                "updated_at": record.updated_at,
            }
        )
    counts: dict[str, int] = {}
    for record in job.records.values():
        counts[record.state.value] = counts.get(record.state.value, 0) + 1
    return {
        "job_id": job.job_id,
        "kind": job.kind,
        "state": job.state.value,
        "plan_version": job.plan_version,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "counts": counts,
        "pending": [
            key for key in job.order if job.records[key].state.value == "pending"
        ],
        "blocked": [
            key
            for key in job.order
            if job.records[key].state.value == "blocked"
        ],
        "done": [
            key for key in job.order if job.records[key].state.value == "done"
        ],
        "steps": steps,
    }
