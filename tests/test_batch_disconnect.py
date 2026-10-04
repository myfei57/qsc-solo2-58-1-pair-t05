"""End-to-end behaviour of the resumable ordered disconnect batch."""

from __future__ import annotations

import tempfile
import unittest

from portpower.batch.model import BatchItem, StepState
from portpower.batch.plan import PlanInvalid, plan_order
from portpower.batch.repository import BatchRepository
from portpower.batch.runner import BatchRunner, BatchStateError
from portpower.disconn.breaker import Breaker
from portpower.disconn.gateway import DisconnectGateway
from portpower.disconn.pool import BreakerPool


class StuckBreaker(Breaker):
    """Accepts the open command but physically stays closed until freed."""

    def __init__(self, identifier: str) -> None:
        super().__init__(identifier)
        self.stuck = True

    def mark_open(self) -> None:
        if self.stuck:
            return None
        super().mark_open()

    def free(self) -> None:
        self.stuck = False


class TrippingBreaker(Breaker):
    """Open command trips the breaker instead of opening cleanly."""

    def mark_open(self) -> None:
        self.mark_tripped()


def build_harness(pool: BreakerPool | None = None):
    directory = tempfile.mkdtemp()
    from portpower.store.kvstore import RecordStore

    store = RecordStore(directory)
    store.load()
    pool = pool or BreakerPool()
    gateway = DisconnectGateway(pool)
    repository = BatchRepository(store)
    runner = BatchRunner(repository, {"disconnect": gateway})
    return store, repository, runner, pool


def item(
    key: str, requires: frozenset[str] | None = None, priority: int = 0
) -> BatchItem:
    return BatchItem(
        key=key, label=key, requires=requires or frozenset(), priority=priority
    )


class OrderedBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store, self.repo, self.runner, self.pool = build_harness()
        for code in ("A1", "A2", "B1", "B2"):
            breaker = Breaker(code)
            breaker.mark_closed()
            self.pool.register(code, breaker)

    def create(self, items: list[BatchItem]):
        return self.repo.create("disconnect", items)

    def test_happy_path_opens_in_order_and_completes(self) -> None:
        job = self.create([item("A1"), item("A2")])
        job = self.runner.advance(job.job_id)

        self.assertEqual(job.state.value, "completed")
        states = [record.state for record in job.ordered_records()]
        self.assertEqual(states, [StepState.DONE, StepState.DONE])
        self.assertFalse(self.pool.get("A1").closed)
        self.assertFalse(self.pool.get("A2").closed)
        seqs = [record.finish_seq for record in job.ordered_records()]
        self.assertEqual(seqs, [1, 2])

    def test_stuck_step_blocks_chain_and_leaves_rest_pending(self) -> None:
        stuck = StuckBreaker("A2")
        stuck.mark_closed()
        self.pool.register("A2", stuck)

        job = self.create([item("A1"), item("A2"), item("B1")])
        job = self.runner.advance(job.job_id)

        by_key = job.records
        self.assertEqual(by_key["A1"].state, StepState.DONE)
        self.assertEqual(by_key["A2"].state, StepState.BLOCKED)
        self.assertIn("stuck", by_key["A2"].detail)
        self.assertEqual(by_key["A2"].physical_after, "closed")
        # Everything behind the jam is explicitly 待处理, not half-run.
        self.assertEqual(by_key["B1"].state, StepState.PENDING)
        self.assertTrue(self.pool.get("B1").closed)
        self.assertEqual(job.state.value, "active")

    def test_resume_does_not_reopen_done_step(self) -> None:
        stuck = StuckBreaker("A2")
        stuck.mark_closed()
        self.pool.register("A2", stuck)

        job = self.create([item("A1"), item("A2"), item("B1")])
        self.runner.advance(job.job_id)

        # Field crew frees the jam; A1 is already open.
        stuck.free()

        # Rerun used to re-open A1 and crash with BreakerNotClosed.
        job = self.runner.advance(job.job_id)

        self.assertEqual(job.state.value, "completed")
        a1 = job.records["A1"]
        self.assertEqual(a1.attempts, 1)  # command issued exactly once
        self.assertTrue(
            any(
                event.state == "done" and "applied" in event.note
                for event in a1.history
            )
        )
        self.assertEqual(job.records["B1"].state, StepState.DONE)

    def test_already_open_breaker_is_reconciled_not_recommanded(self) -> None:
        # New operator reruns the whole batch against live equipment:
        # A1 was opened manually, its record was lost/never written.
        self.pool.get("A1").mark_open()
        job = self.create([item("A1"), item("A2")])
        job = self.runner.advance(job.job_id)

        self.assertEqual(job.state.value, "completed")
        a1 = job.records["A1"]
        self.assertEqual(a1.state, StepState.DONE)
        self.assertEqual(a1.attempts, 0)
        self.assertEqual(a1.physical_before, "open")
        self.assertIn("reconciled", a1.detail)
        self.assertFalse(self.pool.get("A2").closed)

    def test_tripped_breaker_is_distinct_from_stuck(self) -> None:
        tripping = TrippingBreaker("A1")
        tripping.mark_closed()
        self.pool.register("A1", tripping)

        job = self.create([item("A1"), item("A2")])
        job = self.runner.advance(job.job_id)

        self.assertEqual(job.records["A1"].state, StepState.BLOCKED)
        self.assertEqual(job.records["A1"].physical_after, "tripped")
        self.assertIn("trip", job.records["A1"].detail)
        self.assertEqual(job.records["A2"].state, StepState.PENDING)

    def test_manual_retry_then_skip_flow(self) -> None:
        stuck = StuckBreaker("A1")
        stuck.mark_closed()
        self.pool.register("A1", stuck)
        job = self.create([item("A1"), item("A2")])
        self.runner.advance(job.job_id)

        with self.assertRaises(BatchStateError):
            self.runner.retry(job.job_id, "A2")  # not the head

        # Still stuck after a retry: blocks again, A2 untouched.
        job = self.runner.retry(job.job_id, "A1")
        self.assertEqual(job.records["A1"].state, StepState.BLOCKED)
        self.assertEqual(job.records["A1"].attempts, 2)
        self.assertTrue(self.pool.get("A2").closed)

        # Operator verifies on site and explicitly marks it settled.
        job = self.runner.skip(job.job_id, "A1", "field verified open")
        self.assertEqual(job.state.value, "completed")
        self.assertIn("field verified", job.records["A1"].detail)

    def test_running_step_is_recovered_after_restart(self) -> None:
        # Simulate a crash: command issued (attempt recorded, RUNNING),
        # breaker physically opened, but DONE never persisted.
        job = self.create([item("A1"), item("A2")])
        record = job.records["A1"]
        record.attempts = 1
        record.physical_before = "closed"
        record.append_event(StepState.RUNNING, "dispatching")
        self.repo.save(job)
        self.pool.get("A1").mark_open()

        job = self.runner.advance(job.job_id)
        self.assertEqual(job.state.value, "completed")
        self.assertEqual(job.records["A1"].attempts, 1)  # no second command
        self.assertIn("recovered", job.records["A1"].detail)

    def test_running_step_still_closed_is_redispatched(self) -> None:
        job = self.create([item("A1"), item("A2")])
        record = job.records["A1"]
        record.attempts = 1
        record.physical_before = "closed"
        record.append_event(StepState.RUNNING, "dispatching")
        self.repo.save(job)

        job = self.runner.advance(job.job_id)
        self.assertEqual(job.state.value, "completed")
        # The pre-crash attempt plus one safe re-dispatch.
        self.assertEqual(job.records["A1"].attempts, 2)
        self.assertEqual(job.records["A1"].state, StepState.DONE)
        self.assertFalse(self.pool.get("A1").closed)

    def test_running_step_tripped_is_blocked_for_manual_check(self) -> None:
        tripping = TrippingBreaker("A1")
        tripping.mark_tripped()
        self.pool.register("A1", tripping)
        job = self.create([item("A1"), item("A2")])
        record = job.records["A1"]
        record.attempts = 1
        record.physical_before = "closed"
        record.append_event(StepState.RUNNING, "dispatching")
        self.repo.save(job)

        job = self.runner.advance(job.job_id)
        self.assertEqual(job.records["A1"].state, StepState.BLOCKED)
        self.assertEqual(job.records["A1"].physical_after, "tripped")
        self.assertIn("manual check", job.records["A1"].detail)
        self.assertEqual(job.records["A2"].state, StepState.PENDING)


class ReorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store, self.repo, self.runner, self.pool = build_harness()
        for code in ("A1", "A2", "B1", "B2"):
            breaker = Breaker(code)
            breaker.mark_closed()
            self.pool.register(code, breaker)

    def test_constraints_drive_initial_order(self) -> None:
        job = self.repo.create(
            "disconnect",
            [
                item("A1", priority=5),
                item("A2", requires=frozenset({"A1"})),
                item("B1", priority=0),
            ],
        )
        self.assertEqual(job.order, ["B1", "A1", "A2"])

    def test_cycle_is_rejected(self) -> None:
        with self.assertRaises(PlanInvalid):
            self.repo.create(
                "disconnect",
                [
                    item("A1", requires=frozenset({"A2"})),
                    item("A2", requires=frozenset({"A1"})),
                ],
            )

    def test_unknown_constraint_is_rejected(self) -> None:
        with self.assertRaises(PlanInvalid):
            self.repo.create(
                "disconnect", [item("A1", requires=frozenset({"ZZ9"}))]
            )

    def test_reorder_after_progress_keeps_settled_records_intact(self) -> None:
        stuck = StuckBreaker("A2")
        stuck.mark_closed()
        self.pool.register("A2", stuck)
        job = self.repo.create(
            "disconnect", [item("A1"), item("A2"), item("B1"), item("B2")]
        )
        original_order = list(job.order)
        self.runner.advance(job.job_id)

        done_before = {
            key: (
                job.records[key].state,
                job.records[key].finish_seq,
                tuple(event.at for event in job.records[key].history),
            )
            for key in job.order
            if job.records[key].done
        }

        # New safety rule: B2 must open before B1; B1 before A2.
        job = self.runner.update_constraints(
            job.job_id,
            requires={"A2": ["B1"], "B1": ["B2"]},
        )

        order = job.order
        # Settled prefix is untouched and stays first.
        self.assertEqual(order[:1], ["A1"])
        self.assertEqual(job.plan_version, 2)
        # Open work follows the new constraint chain.
        self.assertEqual(order[1:], ["B2", "B1", "A2"])

        # No landed record changed in any way.
        for key, (state, seq, events) in done_before.items():
            record = job.records[key]
            self.assertEqual(record.state, state)
            self.assertEqual(record.finish_seq, seq)
            self.assertEqual(
                tuple(event.at for event in record.history), events
            )
        # Reordering alone never executes equipment.
        self.assertEqual(job.records["B2"].state, StepState.PENDING)

        # Resume now continues in the new order from the breakpoint.
        stuck.free()
        job = self.runner.advance(job.job_id)
        self.assertEqual(job.state.value, "completed")
        seqs = [job.records[key].finish_seq for key in original_order]
        self.assertEqual(job.records["A1"].finish_seq, 1)
        self.assertEqual(job.records["B2"].finish_seq, 2)
        self.assertEqual(job.records["B1"].finish_seq, 3)
        self.assertEqual(job.records["A2"].finish_seq, 4)

    def test_settled_item_constraints_are_frozen(self) -> None:
        stuck = StuckBreaker("A2")
        stuck.mark_closed()
        self.pool.register("A2", stuck)
        job = self.repo.create(
            "disconnect", [item("A1"), item("A2"), item("B1")]
        )
        self.runner.advance(job.job_id)

        with self.assertRaises(BatchStateError):
            self.runner.update_constraints(
                job.job_id, requires={"A1": ["B1"]}
            )
        with self.assertRaises(BatchStateError):
            self.runner.update_constraints(
                job.job_id, priority={"A1": 9}
            )

    def test_cycle_introduced_after_progress_is_rejected(self) -> None:
        stuck = StuckBreaker("A2")
        stuck.mark_closed()
        self.pool.register("A2", stuck)
        job = self.repo.create(
            "disconnect", [item("A1"), item("A2"), item("B1")]
        )
        self.runner.advance(job.job_id)
        with self.assertRaises(PlanInvalid):
            self.runner.update_constraints(
                job.job_id, requires={"A2": ["B1"], "B1": ["A2"]}
            )

    def test_plan_order_helper_freezes_done_by_finish_sequence(self) -> None:
        items = {
            "A1": item("A1"),
            "A2": item("A2"),
            "B1": item("B1", requires=frozenset({"A2"})),
        }
        from portpower.batch.model import StepRecord

        records = {
            key: StepRecord(key=key) for key in items
        }
        records["A2"].state = StepState.DONE
        records["A2"].finish_seq = 1
        records["A1"].state = StepState.DONE
        records["A1"].finish_seq = 2
        ordered = plan_order(items, records)
        self.assertEqual(ordered, ["A2", "A1", "B1"])


class PersistenceTests(unittest.TestCase):
    def test_batch_survives_repository_reload(self) -> None:
        directory = tempfile.mkdtemp()
        from portpower.store.kvstore import RecordStore

        store = RecordStore(directory)
        store.load()
        pool = BreakerPool()
        stuck = StuckBreaker("A2")
        stuck.mark_closed()
        pool.register("A1", _closed("A1"))
        pool.register("A2", stuck)
        gateway = DisconnectGateway(pool)
        repo = BatchRepository(store)
        runner = BatchRunner(repo, {"disconnect": gateway})

        job = repo.create("disconnect", [item("A1"), item("A2"), item("B1")])
        runner.advance(job.job_id)
        pool.register("B1", _closed("B1"))

        # Simulate process restart: fresh store load, fresh collaborators.
        store2 = RecordStore(directory)
        store2.load()
        pool2 = BreakerPool()
        stuck2 = StuckBreaker("A2")
        stuck2.mark_closed()
        pool2.register("A1", Breaker("A1"))  # already open physically
        pool2.register("A2", stuck2)
        pool2.register("B1", _closed("B1"))
        repo2 = BatchRepository(store2)
        runner2 = BatchRunner(
            repo2, {"disconnect": DisconnectGateway(pool2)}
        )

        reloaded = repo2.load(job.job_id)
        self.assertEqual(reloaded.records["A1"].state, StepState.DONE)
        self.assertEqual(reloaded.records["A2"].state, StepState.BLOCKED)
        self.assertEqual(reloaded.records["B1"].state, StepState.PENDING)
        self.assertEqual(repo2.list_ids(), [job.job_id])

        stuck2.free()
        finished = runner2.advance(job.job_id)
        self.assertEqual(finished.state.value, "completed")
        self.assertEqual(finished.records["A1"].attempts, 1)

    def test_completed_batch_cannot_advance_again(self) -> None:
        store, repo, runner, pool = build_harness()
        pool.register("A1", _closed("A1"))
        job = repo.create("disconnect", [item("A1")])
        runner.advance(job.job_id)
        with self.assertRaises(BatchStateError):
            runner.advance(job.job_id)


def _closed(identifier: str) -> Breaker:
    breaker = Breaker(identifier)
    breaker.mark_closed()
    return breaker


if __name__ == "__main__":
    unittest.main()
