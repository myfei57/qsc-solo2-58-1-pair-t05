"""Registry of per-berth shore breakers used by ordered batch work."""

from __future__ import annotations

import threading

from portpower.disconn.breaker import Breaker


class BreakerPool:
    """Holds one breaker per stable key (berth code / breaker id)."""

    def __init__(self) -> None:
        self._breakers: dict[str, Breaker] = {}
        self._guard = threading.Lock()

    def register(self, key: str, breaker: Breaker) -> None:
        with self._guard:
            self._breakers[key] = breaker

    def get(self, key: str) -> Breaker:
        with self._guard:
            breaker = self._breakers.get(key)
            if breaker is None:
                breaker = Breaker(key)
                breaker.mark_closed()
                self._breakers[key] = breaker
            return breaker

    def has(self, key: str) -> bool:
        with self._guard:
            return key in self._breakers

    def keys(self) -> list[str]:
        with self._guard:
            return sorted(self._breakers)
