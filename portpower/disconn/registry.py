"""One breaker per berth with durable state.

The shore-main breaker is a single shared unit, which is why the old batch
loop could not tell which berths had actually opened. This registry gives
every berth its own unit whose state survives restarts, so "already opened"
is a fact the system knows instead of something the crew has to pace off.
"""

from __future__ import annotations

from portpower.disconn.breaker import Breaker
from portpower.disconn.state import BreakerState
from portpower.store.kvstore import RecordStore

PREFIX = "breaker.berth."


def breaker_key(berth: str) -> str:
    return PREFIX + berth


class BreakerRegistry:
    def __init__(self, store: RecordStore) -> None:
        self._store = store

    def for_berth(self, berth: str) -> Breaker:
        breaker = Breaker(f"berth-{berth}")
        state = self._store.get_json(breaker_key(berth))
        if state == BreakerState.CLOSED.value:
            breaker.mark_closed()
        elif state == BreakerState.TRIPPED.value:
            breaker.mark_tripped()
        return breaker

    def save(self, berth: str, breaker: Breaker) -> None:
        self._store.put_json(breaker_key(berth), breaker.state.value)

    def ensure_closed(self, berth: str) -> None:
        """A bound berth is connected, so its breaker starts out closed."""

        if not self._store.has(breaker_key(berth)):
            self.mark_closed(berth)

    def mark_closed(self, berth: str) -> None:
        breaker = self.for_berth(berth)
        breaker.mark_closed()
        self.save(berth, breaker)

    def mark_open(self, berth: str) -> None:
        breaker = self.for_berth(berth)
        breaker.mark_open()
        self.save(berth, breaker)

    def state_for(self, berth: str) -> str:
        return self.for_berth(berth).state.value
