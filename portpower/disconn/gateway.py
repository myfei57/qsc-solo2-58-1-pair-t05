"""Batch gateway that opens shore breakers one berth at a time.

Unlike the fire-and-forget :func:`portpower.disconn.batch.batch_disconnect`
loop, this gateway:

* reads the breaker's real state before issuing anything, so a breaker
  that is already open is reconciled instead of being "opened" again;
* verifies the physical result after the command, distinguishing a
  stuck breaker (still closed) from a tripped one;
* performs the meter final-read bookkeeping idempotently.
"""

from __future__ import annotations

from dataclasses import dataclass

from portpower.batch.model import PHYSICAL_OPEN, BatchItem
from portpower.batch.runner import StepOutcome
from portpower.disconn.pool import BreakerPool


@dataclass(frozen=True)
class DisconnectTarget:
    key: str
    label: str
    requires: frozenset[str] = frozenset()
    priority: int = 0
    final_value: float = 0.0


class DisconnectGateway:
    """StepGateway for the ``disconnect`` batch kind."""

    kind = "disconnect"

    def __init__(self, pool: BreakerPool, binder=None) -> None:
        self._pool = pool
        self._binder = binder

    def observe(self, key: str, item: BatchItem) -> str:
        return self._physical(self._pool.get(key))

    def apply(self, key: str, item: BatchItem) -> StepOutcome:
        # The runner only dispatches after observe() reported a non-open,
        # non-tripped breaker (i.e. closed); mark_open is the single command.
        breaker = self._pool.get(key)
        breaker.mark_open()
        after = self._physical(breaker)
        if after == PHYSICAL_OPEN:
            self._bookkeeping(key, item)
        return StepOutcome(
            physical_after=after,
            applied=after == PHYSICAL_OPEN,
            note="open command applied" if after == PHYSICAL_OPEN else "",
        )

    def _bookkeeping(self, key: str, item: BatchItem) -> None:
        if self._binder is None:
            return
        if self._binder.load(key) is None:
            # Nothing to settle for a berth that was never bound.
            return
        final_value = item.extra.get("final_value")
        if not isinstance(final_value, (int, float)) or isinstance(
            final_value, bool
        ):
            final_value = 0.0
        self._binder.record_final(key, float(final_value))
        self._binder.unbind(key)

    @staticmethod
    def _physical(breaker) -> str:
        return breaker.state.value
