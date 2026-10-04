"""Plan ordering under safety constraints.

Only unfinished items are ever ordered: settled results keep their
historical completion position and are simply placed first, so adjusting
the plan can never rewrite a landed record.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from portpower.batch.model import BatchItem, StepRecord


class PlanInvalid(ValueError):
    """Raised when constraints cannot produce a safe order."""


def topological_order(
    items: Mapping[str, BatchItem],
    keys: Iterable[str],
) -> list[str]:
    """Return ``keys`` ordered so that every ``requires`` edge points back.

    Ties break by ``(priority, key)`` to keep the order deterministic.
    Unknown dependencies and cycles raise :class:`PlanInvalid`.
    """

    pending = set(keys)
    unknown: set[str] = set()
    for key in pending:
        item = items.get(key)
        if item is None:
            unknown.add(key)
            continue
        missing = item.requires - set(items)
        if missing:
            unknown.update(missing)
    if unknown:
        raise PlanInvalid(
            "unknown item keys in constraints: " + ", ".join(sorted(unknown))
        )

    indegree = {
        key: len(items[key].requires & pending) for key in pending
    }
    dependents: dict[str, set[str]] = {key: set() for key in pending}
    for key in pending:
        for prerequisite in items[key].requires:
            if prerequisite in pending:
                dependents[prerequisite].add(key)

    ready = sorted(
        (key for key in pending if indegree[key] == 0),
        key=lambda candidate: (items[candidate].priority, candidate),
    )
    ordered: list[str] = []
    while ready:
        key = ready.pop(0)
        ordered.append(key)
        for dependent in sorted(dependents[key]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
                ready.sort(
                    key=lambda candidate: (
                        items[candidate].priority,
                        candidate,
                    )
                )

    if len(ordered) != len(pending):
        stuck = sorted(pending - set(ordered))
        raise PlanInvalid(
            "safety constraints contain a cycle through: " + ", ".join(stuck)
        )
    return ordered


def plan_order(
    items: Mapping[str, BatchItem],
    records: Mapping[str, StepRecord],
) -> list[str]:
    """Order the plan while preserving settled history.

    Settled items come first in finish order; everything open is ordered
    by the safety constraints. Reordering therefore changes only where
    the unfinished work resumes.
    """

    settled = sorted(
        (key for key, record in records.items() if record.done),
        key=lambda candidate: records[candidate].finish_seq
        if records[candidate].finish_seq is not None
        else 0,
    )
    open_keys = [key for key in items if not records[key].done]
    return settled + topological_order(items, open_keys)
