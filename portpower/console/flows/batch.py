"""Multi-ship departure batches: ordered, recorded, resumable.

One batch opens the berth breakers one at a time, in the planned order,
and persists the outcome of every step before moving on. A stuck step
stops the batch at that berth — the breakpoint — and a later resume picks
up exactly there. Steps that already settled are never executed again;
steps still waiting stay marked pending in the batch view.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from portpower.console.http import BadRequest
from portpower.console.validation import optional_number, require_text, valid_berth_code
from portpower.disconn.open import open_breaker
from portpower.disconn.plan import DepartureBatch, PlannedStep
from portpower.meter.binder import BindingMissing
from portpower.meter.freeze import freeze
from portpower.meter.meter import Meter


@dataclass(frozen=True)
class BatchStepRequest:
    berth: str
    final_value: float


@dataclass(frozen=True)
class BatchCreateRequest:
    steps: list[BatchStepRequest]
    run: bool

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BatchCreateRequest":
        raw_steps = payload.get("steps")
        if not isinstance(raw_steps, list) or not raw_steps:
            raise BadRequest("steps must be a non-empty list")
        steps: list[BatchStepRequest] = []
        for item in raw_steps:
            if not isinstance(item, dict):
                raise BadRequest("each step must be an object")
            steps.append(
                BatchStepRequest(
                    berth=require_text(item, "berth"),
                    final_value=optional_number(item, "final_value", 0.0),
                )
            )
        run = payload.get("run", True)
        if not isinstance(run, bool):
            raise BadRequest("run must be a boolean")
        return cls(steps=steps, run=run)


@dataclass(frozen=True)
class BatchResumeRequest:
    batch_id: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BatchResumeRequest":
        return cls(batch_id=require_text(payload, "id"))


@dataclass(frozen=True)
class BatchReorderRequest:
    batch_id: str
    order: list[str]

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BatchReorderRequest":
        raw_order = payload.get("order")
        if not isinstance(raw_order, list) or not raw_order:
            raise BadRequest("order must be a non-empty list of berth codes")
        order: list[str] = []
        for item in raw_order:
            if not isinstance(item, str) or not item.strip():
                raise BadRequest("order must be a non-empty list of berth codes")
            order.append(item.strip())
        return cls(batch_id=require_text(payload, "id"), order=order)


def create_batch(runtime, request: BatchCreateRequest) -> dict[str, object]:
    steps = _plan(runtime, request.steps)
    batch = DepartureBatch.create(steps)
    runtime.batches.save(batch)
    if request.run:
        batch = _run(runtime, batch.batch_id)
    return batch.view()


def resume_batch(runtime, request: BatchResumeRequest) -> dict[str, object]:
    return _run(runtime, request.batch_id).view()


def reorder_batch(runtime, request: BatchReorderRequest) -> dict[str, object]:
    with runtime.batches.locked():
        batch = runtime.batches.load(request.batch_id)
        batch.reorder(request.order)
        runtime.batches.save(batch)
        return batch.view()


def batch_view(runtime, batch_id: str) -> dict[str, object]:
    return runtime.batches.load(batch_id).view()


def list_batches(runtime) -> dict[str, object]:
    batches = runtime.batches.list()
    return {
        "count": len(batches),
        "batches": [batch.summary() for batch in batches],
    }


def _plan(runtime, requested: list[BatchStepRequest]) -> list[PlannedStep]:
    steps: list[PlannedStep] = []
    seen: set[str] = set()
    for item in requested:
        if not valid_berth_code(item.berth) or not runtime.catalog.has(item.berth):
            raise BadRequest(f"invalid or unknown berth code {item.berth!r}")
        if item.berth in seen:
            raise BadRequest(f"duplicate berth {item.berth}")
        seen.add(item.berth)
        binding = runtime.binder.load(item.berth)
        if binding is None:
            raise BindingMissing(f"no meter binding for berth {item.berth}")
        runtime.breakers.ensure_closed(item.berth)
        steps.append(
            PlannedStep(
                berth=item.berth,
                ship=binding.ship,
                final_value=item.final_value,
            )
        )
    return steps


def _run(runtime, batch_id: str) -> DepartureBatch:
    """Walk the queue from its head, persisting every settled step at once.

    A failure is recorded on the batch and stops the walk; it is state the
    crew needs to see, not an exception that hides what already happened.
    """

    with runtime.batches.locked():
        batch = runtime.batches.load(batch_id)
        while True:
            step = batch.next_step()
            if step is None:
                return batch
            try:
                final, detail = _execute_step(runtime, batch.batch_id, step)
            except Exception as exc:
                batch.record_failure(step.berth, str(exc))
                runtime.batches.save(batch)
                return batch
            batch.record_done(step.berth, final, detail)
            runtime.batches.save(batch)


def _execute_step(runtime, batch_id: str, step: PlannedStep) -> tuple[float, str]:
    """Open one berth breaker and settle its metering and occupancy.

    A breaker found already open means a previous attempt died mid-step (or
    the crew opened it by hand); the remaining sub-steps are idempotent, so
    the step simply completes and the record says what was found.
    """

    breaker = runtime.breakers.for_berth(step.berth)
    detail = "already-open"
    if breaker.closed:
        open_breaker(breaker)
        runtime.breakers.save(step.berth, breaker)
        detail = "opened"
    final = runtime.binder.record_final(step.berth, step.final_value)
    runtime.binder.unbind(step.berth)

    meter = Meter(f"meter-{step.berth}", step.berth, runtime.store)
    freeze(meter)

    berth = runtime.berths.get(step.berth)
    if berth is not None and berth.ship_id == step.ship:
        runtime.berths.release(step.berth)
    runtime.fleet.undock(step.ship)
    runtime.mapping.unbind(step.berth)
    runtime.ledger.append(
        "disconnect",
        f"{runtime.harbor.qualified(step.berth)} {step.ship} {batch_id}",
    )
    return final, detail
