import asyncio

from memebot.bus import EventBus
from memebot.events import LifecycleTransition, SafetyHardFail
from memebot.safety.gate import GateRunner
from memebot.store import open_db, upsert_token


class FakeGate:
    """Stub gate: returns a canned report per call; records mints evaluated."""

    def __init__(self, reports_by_mint):
        self._reports = reports_by_mint
        self.evaluated = []

    async def evaluate_unpersisted(self, token):
        self.evaluated.append(token["mint"])
        return self._reports[token["mint"]]

    def persist(self, draft):
        return draft


class FakeReport:
    def __init__(self, passed, hard_fails=(), report_id=None):
        self.passed = passed
        self.hard_fails = hard_fails
        self.report_id = report_id


async def test_hardfail_on_climbing_publishes_safety_hard_fail(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M1", created_at=1.0)
    bus = EventBus()
    hard_fails_q = bus.subscribe(SafetyHardFail)
    gate = FakeGate({"M1": FakeReport(passed=False, hard_fails=("mint_authority_active",))})
    runner = GateRunner(bus, conn, gate)
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))

    await bus.publish(LifecycleTransition(t_wall=1.0, t_mono=1.0, mint="M1",
                                          from_state="FRESH", to_state="CLIMBING"))
    ev = await asyncio.wait_for(hard_fails_q.get(), 5)
    assert (ev.mint, ev.reasons) == ("M1", ("mint_authority_active",))
    assert gate.evaluated == ["M1"]
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_fresh_transition_is_not_evaluated(tmp_path):
    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M2", created_at=1.0)
    bus = EventBus()
    hard_fails_q = bus.subscribe(SafetyHardFail)
    gate = FakeGate({})
    runner = GateRunner(bus, conn, gate)
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))

    await bus.publish(LifecycleTransition(t_wall=1.0, t_mono=1.0, mint="M2",
                                          from_state="FRESH", to_state="FRESH"))
    await asyncio.sleep(0.1)
    assert gate.evaluated == []
    assert hard_fails_q.empty()
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_gate_runner_emits_safety_passed_on_pass(tmp_path):
    import time
    from memebot.bus import EventBus
    from memebot.events import LifecycleTransition, SafetyPassed
    from memebot.safety.gate import GateRunner
    from memebot.store import open_db, upsert_token, set_token_state

    class PassGate:
        async def evaluate_unpersisted(self, token):
            from memebot.safety.gate import SafetyReport
            return SafetyReport(mint=token["mint"], checked_at=1.0, segment=token["state"],
                                hard_fails=(), risk_score=15.0, results=(),
                                inputs_hash="h", report_id=42)

        def persist(self, draft):
            return draft

    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M", created_at=1.0)
    set_token_state(conn, "M", "CLIMBING")
    bus = EventBus()
    q = bus.subscribe(SafetyPassed)
    runner = GateRunner(bus, conn, PassGate())
    import asyncio
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))
    await bus.publish(LifecycleTransition(t_wall=time.time(), t_mono=time.monotonic(),
                                          mint="M", from_state="FRESH", to_state="CLIMBING"))
    ev = await asyncio.wait_for(q.get(), 2)
    assert ev.mint == "M" and ev.safety_report_id == 42 and ev.risk_score == 15.0
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_gate_runner_uses_evaluate_once_then_persist_once(tmp_path):
    draft = object()
    persisted = asyncio.Event()

    class TwoPhaseGate:
        def __init__(self):
            self.calls = []

        async def evaluate_unpersisted(self, token):
            self.calls.append(("evaluate_unpersisted", token["mint"]))
            return draft

        def persist(self, evaluated):
            self.calls.append(("persist", evaluated))
            persisted.set()
            return FakeReport(passed=False, hard_fails=("test_hard_fail",))

    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="M3", created_at=1.0)
    bus = EventBus()
    gate = TwoPhaseGate()
    runner = GateRunner(bus, conn, gate)
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))

    try:
        await bus.publish(LifecycleTransition(
            t_wall=1.0,
            t_mono=1.0,
            mint="M3",
            from_state="FRESH",
            to_state="CLIMBING",
        ))
        await asyncio.wait_for(persisted.wait(), 1)
        assert gate.calls == [
            ("evaluate_unpersisted", "M3"),
            ("persist", draft),
        ]
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_gate_runner_events_carry_persisted_report_id(tmp_path):
    from memebot.events import SafetyPassed
    from memebot.safety.gate import SafetyReport

    conn = open_db(tmp_path / "t.db")
    upsert_token(conn, mint="FAIL", created_at=1.0)
    upsert_token(conn, mint="PASS", created_at=1.0)
    bus = EventBus()
    hard_fails_q = bus.subscribe(SafetyHardFail)
    passed_q = bus.subscribe(SafetyPassed)
    gate = FakeGate({
        "FAIL": SafetyReport(
            mint="FAIL",
            checked_at=1.0,
            segment="CLIMBING",
            hard_fails=("mint_authority_active",),
            risk_score=100.0,
            results=(),
            inputs_hash="f" * 64,
            report_id=101,
        ),
        "PASS": SafetyReport(
            mint="PASS",
            checked_at=1.0,
            segment="CLIMBING",
            hard_fails=(),
            risk_score=10.0,
            results=(),
            inputs_hash="a" * 64,
            report_id=102,
        ),
    })
    runner = GateRunner(bus, conn, gate)
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))

    try:
        for mint in ("FAIL", "PASS"):
            await bus.publish(LifecycleTransition(
                t_wall=1.0,
                t_mono=1.0,
                mint=mint,
                from_state="FRESH",
                to_state="CLIMBING",
            ))

        hard_fail = await asyncio.wait_for(hard_fails_q.get(), 1)
        passed = await asyncio.wait_for(passed_q.get(), 1)
        assert hard_fail.safety_report_id == 101
        assert passed.safety_report_id == 102
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


async def test_gate_runner_critical_subscription_unsubscribes_in_finally(
    tmp_path, monkeypatch,
):
    import pytest

    from memebot.events import SafetyPassed
    from memebot.safety.gate import SafetyReport

    conn = open_db(tmp_path / "t.db")
    for mint in (
        "NONGATED",
        "PASS",
        "FAIL",
        "BLOCKED",
        "EVALUATION_FAILURE",
        "PUBLICATION_FAILURE",
        "CANCELLED",
    ):
        upsert_token(conn, mint=mint, created_at=1.0)

    bus = EventBus()
    passed_q = bus.subscribe(SafetyPassed)
    hard_fails_q = bus.subscribe(SafetyHardFail)
    gate = FakeGate({
        "PASS": SafetyReport(
            mint="PASS",
            checked_at=1.0,
            segment="CLIMBING",
            hard_fails=(),
            risk_score=10.0,
            results=(),
            inputs_hash="a" * 64,
            report_id=101,
        ),
        "FAIL": SafetyReport(
            mint="FAIL",
            checked_at=1.0,
            segment="CLIMBING",
            hard_fails=("mint_authority_active",),
            risk_score=100.0,
            results=(),
            inputs_hash="f" * 64,
            report_id=102,
        ),
    })
    runner = GateRunner(bus, conn, gate)
    subscription = next(item for item in bus._subs if item.queue is runner._q)
    acknowledged = []
    original_critical_done = bus.critical_done

    def observed_critical_done(queue):
        acknowledged.append(queue)
        original_critical_done(queue)

    monkeypatch.setattr(bus, "critical_done", observed_critical_done)
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))

    for mint, to_state in (
        ("NONGATED", "FRESH"),
        ("MISSING", "CLIMBING"),
        ("PASS", "CLIMBING"),
        ("FAIL", "CLIMBING"),
    ):
        await bus.publish(LifecycleTransition(
            t_wall=1.0,
            t_mono=1.0,
            mint=mint,
            from_state="FRESH",
            to_state=to_state,
        ))

    passed = await asyncio.wait_for(passed_q.get(), 1)
    hard_fail = await asyncio.wait_for(hard_fails_q.get(), 1)
    passed_q.task_done()
    hard_fails_q.task_done()
    assert passed.mint == "PASS"
    assert hard_fail.mint == "FAIL"

    stop.set()
    await asyncio.wait_for(task, 2)

    assert subscription not in bus._subs
    assert subscription.closed is True
    assert subscription.close_event.is_set() is True
    assert subscription.critical is True
    assert acknowledged == [runner._q] * 4
    await asyncio.wait_for(runner._q.join(), 1)
    assert bus.critical_state() == (4, 0, False)

    blocked_bus = EventBus(maxsize=1)
    blocked_passed_q = blocked_bus.subscribe(SafetyPassed)
    blocked_passed_q.put_nowait(SafetyPassed(
        t_wall=0.0,
        t_mono=0.0,
        mint="BLOCKER",
        segment="CLIMBING",
        safety_report_id=1,
        risk_score=1.0,
    ))
    blocked_gate = FakeGate({
        "BLOCKED": SafetyReport(
            mint="BLOCKED",
            checked_at=1.0,
            segment="CLIMBING",
            hard_fails=(),
            risk_score=10.0,
            results=(),
            inputs_hash="b" * 64,
            report_id=103,
        ),
    })
    blocked_runner = GateRunner(blocked_bus, conn, blocked_gate)
    blocked_subscription = next(
        item for item in blocked_bus._subs if item.queue is blocked_runner._q
    )
    blocked_acknowledged = []
    blocked_original_done = blocked_bus.critical_done

    def observed_blocked_done(queue):
        blocked_acknowledged.append(queue)
        blocked_original_done(queue)

    monkeypatch.setattr(blocked_bus, "critical_done", observed_blocked_done)
    blocked_publish_started = asyncio.Event()
    blocked_original_publish = blocked_bus.publish

    async def observed_blocked_publish(event):
        if isinstance(event, SafetyPassed):
            blocked_publish_started.set()
        await blocked_original_publish(event)

    monkeypatch.setattr(blocked_bus, "publish", observed_blocked_publish)
    blocked_stop = asyncio.Event()
    blocked_baseline_tasks = set(asyncio.all_tasks())
    blocked_task = asyncio.create_task(blocked_runner.run(blocked_stop))
    await blocked_bus.publish(LifecycleTransition(
        t_wall=1.0,
        t_mono=1.0,
        mint="BLOCKED",
        from_state="FRESH",
        to_state="CLIMBING",
    ))
    await asyncio.wait_for(blocked_publish_started.wait(), 1)

    assert blocked_acknowledged == []
    assert blocked_bus.critical_state() == (1, 1, False)
    blocker = blocked_passed_q.get_nowait()
    assert blocker.mint == "BLOCKER"
    blocked_passed_q.task_done()
    await asyncio.wait_for(blocked_runner._q.join(), 1)
    delivered = blocked_passed_q.get_nowait()
    assert delivered.mint == "BLOCKED"
    blocked_passed_q.task_done()
    assert blocked_acknowledged == [blocked_runner._q]
    assert blocked_bus.critical_state() == (1, 0, False)

    blocked_stop.set()
    await asyncio.wait_for(blocked_task, 2)
    await asyncio.sleep(0)
    assert blocked_subscription.closed is True
    assert blocked_subscription not in blocked_bus._subs
    assert asyncio.all_tasks() <= blocked_baseline_tasks

    async def assert_failed_delivery(stage):
        failure_bus = EventBus()
        failure_stop = asyncio.Event()
        evaluation_started = asyncio.Event()
        hold_evaluation = asyncio.Event()
        calls = []

        class FailingGate:
            async def evaluate_unpersisted(self, token):
                calls.append("evaluate")
                evaluation_started.set()
                if stage == "evaluation":
                    raise RuntimeError("controlled evaluation failure")
                if stage == "cancellation":
                    await hold_evaluation.wait()
                return SafetyReport(
                    mint=token["mint"],
                    checked_at=1.0,
                    segment="CLIMBING",
                    hard_fails=(),
                    risk_score=10.0,
                    results=(),
                    inputs_hash="c" * 64,
                    report_id=104,
                )

            def persist(self, draft):
                calls.append("persist")
                return draft

        failure_runner = GateRunner(failure_bus, conn, FailingGate())
        failure_subscription = next(
            item for item in failure_bus._subs
            if item.queue is failure_runner._q
        )
        failure_acknowledged = []
        failure_original_done = failure_bus.critical_done

        def observed_failure_done(queue):
            failure_acknowledged.append(queue)
            failure_original_done(queue)

        monkeypatch.setattr(
            failure_bus, "critical_done", observed_failure_done,
        )
        failure_original_publish = failure_bus.publish

        async def fail_verdict_publish(event):
            if isinstance(event, SafetyPassed):
                calls.append("publish")
                raise RuntimeError("controlled publication failure")
            await failure_original_publish(event)

        if stage == "publication":
            monkeypatch.setattr(failure_bus, "publish", fail_verdict_publish)

        baseline_tasks = set(asyncio.all_tasks())
        await failure_bus.publish(LifecycleTransition(
            t_wall=1.0,
            t_mono=1.0,
            mint={
                "evaluation": "EVALUATION_FAILURE",
                "publication": "PUBLICATION_FAILURE",
                "cancellation": "CANCELLED",
            }[stage],
            from_state="FRESH",
            to_state="CLIMBING",
        ))
        await failure_bus.publish(LifecycleTransition(
            t_wall=2.0,
            t_mono=2.0,
            mint="NONGATED",
            from_state="FRESH",
            to_state="FRESH",
        ))
        failure_task = asyncio.create_task(
            failure_runner.run(failure_stop),
        )
        await asyncio.wait_for(evaluation_started.wait(), 1)

        if stage == "cancellation":
            failure_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(failure_task, 1)
        else:
            with pytest.raises(
                RuntimeError, match=f"controlled {stage} failure",
            ):
                await asyncio.wait_for(failure_task, 1)

        expected_calls = {
            "evaluation": ["evaluate"],
            "publication": ["evaluate", "persist", "publish"],
            "cancellation": ["evaluate"],
        }[stage]
        assert calls == expected_calls
        assert failure_acknowledged == []
        assert failure_runner._q.qsize() == 1
        successor = failure_runner._q.get_nowait()
        assert successor.mint == "NONGATED"
        assert failure_subscription.critical_outstanding == 2
        assert failure_subscription.closed is True
        assert failure_subscription.close_event.is_set() is True
        assert failure_subscription not in failure_bus._subs
        assert failure_bus.critical_state() == (2, 2, True)
        await asyncio.wait_for(
            failure_bus.wait_critical_idle_or_failed(), 1,
        )
        await asyncio.sleep(0)
        assert asyncio.all_tasks() <= baseline_tasks

    for stage in ("publication", "evaluation", "cancellation"):
        await assert_failed_delivery(stage)


async def test_gate_persistence_fail_once_retries_same_draft_without_reprobe(
    tmp_path, caplog,
):
    import sqlite3

    from memebot.events import SafetyPassed
    from memebot.safety.gate import SafetyReport

    conn = open_db(tmp_path / "t.db")
    for mint in ("HELD", "NEXT"):
        upsert_token(conn, mint=mint, created_at=1.0)

    held_draft = object()
    next_draft = object()
    reports = {
        held_draft: SafetyReport(
            mint="HELD",
            checked_at=2.0,
            segment="CLIMBING",
            hard_fails=(),
            risk_score=10.0,
            results=(),
            inputs_hash="a" * 64,
            report_id=101,
        ),
        next_draft: SafetyReport(
            mint="NEXT",
            checked_at=3.0,
            segment="CLIMBING",
            hard_fails=(),
            risk_score=20.0,
            results=(),
            inputs_hash="b" * 64,
            report_id=102,
        ),
    }

    class FailOnceGate:
        def __init__(self):
            self.evaluations = []
            self.persisted_drafts = []

        async def evaluate_unpersisted(self, token):
            self.evaluations.append(token["mint"])
            return held_draft if token["mint"] == "HELD" else next_draft

        def persist(self, draft):
            self.persisted_drafts.append(draft)
            if draft is held_draft and self.persisted_drafts.count(held_draft) == 1:
                raise sqlite3.OperationalError("controlled persistence failure")
            return reports[draft]

    retry_started = asyncio.Event()
    release_retry = asyncio.Event()
    retry_delays = []

    async def retry_sleep(delay):
        retry_delays.append(delay)
        retry_started.set()
        await release_retry.wait()

    bus = EventBus()
    passed_q = bus.subscribe(SafetyPassed)
    gate = FailOnceGate()
    runner = GateRunner(bus, conn, gate)
    # Exercise the missing behavior before asserting the new constructor ABI so RED
    # proves the verdict is dropped, rather than stopping at an argument mismatch.
    runner._retry_sleep = retry_sleep
    stop = asyncio.Event()
    caplog.set_level("ERROR", logger="memebot.safety.gate")

    await bus.publish(LifecycleTransition(
        t_wall=1.0,
        t_mono=1.0,
        mint="HELD",
        from_state="FRESH",
        to_state="CLIMBING",
    ))
    await bus.publish(LifecycleTransition(
        t_wall=2.0,
        t_mono=2.0,
        mint="NEXT",
        from_state="FRESH",
        to_state="CLIMBING",
    ))
    task = asyncio.create_task(runner.run(stop))
    retry_waiter = asyncio.create_task(retry_started.wait())

    try:
        done, _pending = await asyncio.wait(
            (task, retry_waiter), timeout=1, return_when=asyncio.FIRST_COMPLETED,
        )
        assert retry_waiter in done and retry_started.is_set()
        assert not task.done()
        assert gate.evaluations == ["HELD"]
        assert gate.persisted_drafts == [held_draft]
        assert runner._q.qsize() == 1
        assert passed_q.empty()
        assert bus.critical_state() == (2, 2, False)
        assert retry_delays == [0.05]
        assert [record.extra_fields for record in caplog.records] == [
            {"mint": "HELD", "attempt": 1},
        ]

        release_retry.set()
        first = await asyncio.wait_for(passed_q.get(), 1)
        second = await asyncio.wait_for(passed_q.get(), 1)
        assert [(first.mint, first.safety_report_id),
                (second.mint, second.safety_report_id)] == [
            ("HELD", 101),
            ("NEXT", 102),
        ]
        passed_q.task_done()
        passed_q.task_done()
        await asyncio.wait_for(runner._q.join(), 1)
        assert gate.evaluations == ["HELD", "NEXT"]
        assert gate.persisted_drafts == [held_draft, held_draft, next_draft]
        assert bus.critical_state() == (2, 0, False)

        injected_bus = EventBus()
        injected_runner = GateRunner(
            injected_bus, conn, gate, retry_sleep=retry_sleep,
        )
        assert injected_runner._retry_sleep is retry_sleep
        injected_bus.unsubscribe(injected_runner._q)
    finally:
        stop.set()
        release_retry.set()
        if not retry_waiter.done():
            retry_waiter.cancel()
        await asyncio.gather(retry_waiter, return_exceptions=True)
        if not task.done():
            await asyncio.wait_for(task, 2)
        else:
            await asyncio.gather(task, return_exceptions=True)


async def test_gate_persistent_failure_blocks_next_and_shutdown_until_release(
    tmp_path,
):
    import sqlite3

    from memebot.events import SafetyPassed
    from memebot.safety.gate import SafetyReport

    conn = open_db(tmp_path / "t.db")
    for mint in ("HELD", "NEXT"):
        upsert_token(conn, mint=mint, created_at=1.0)

    held_draft = object()
    next_draft = object()
    reports = {
        held_draft: SafetyReport(
            mint="HELD",
            checked_at=2.0,
            segment="CLIMBING",
            hard_fails=(),
            risk_score=10.0,
            results=(),
            inputs_hash="a" * 64,
            report_id=101,
        ),
        next_draft: SafetyReport(
            mint="NEXT",
            checked_at=3.0,
            segment="CLIMBING",
            hard_fails=(),
            risk_score=20.0,
            results=(),
            inputs_hash="b" * 64,
            report_id=102,
        ),
    }
    persistence_released = asyncio.Event()
    retry_started = asyncio.Event()

    class PersistentlyFailingGate:
        def __init__(self):
            self.evaluations = []
            self.persisted_drafts = []

        async def evaluate_unpersisted(self, token):
            self.evaluations.append(token["mint"])
            return held_draft if token["mint"] == "HELD" else next_draft

        def persist(self, draft):
            self.persisted_drafts.append(draft)
            if draft is held_draft and not persistence_released.is_set():
                raise sqlite3.OperationalError("persistent controlled failure")
            return reports[draft]

    async def retry_sleep(delay):
        assert delay == 0.05
        retry_started.set()
        await persistence_released.wait()

    bus = EventBus()
    passed_q = bus.subscribe(SafetyPassed)
    gate = PersistentlyFailingGate()
    runner = GateRunner(bus, conn, gate, retry_sleep=retry_sleep)
    stop = asyncio.Event()
    task = asyncio.create_task(runner.run(stop))
    idle_waiter = None

    try:
        for offset, mint in enumerate(("HELD", "NEXT"), start=1):
            await bus.publish(LifecycleTransition(
                t_wall=float(offset),
                t_mono=float(offset),
                mint=mint,
                from_state="FRESH",
                to_state="CLIMBING",
            ))

        await asyncio.wait_for(retry_started.wait(), 1)
        idle_waiter = asyncio.create_task(bus.wait_critical_idle_or_failed())
        await asyncio.sleep(0)

        assert not task.done()
        assert not idle_waiter.done()
        assert gate.evaluations == ["HELD"]
        assert gate.persisted_drafts == [held_draft]
        assert runner._q.qsize() == 1
        assert passed_q.empty()
        assert bus.critical_state() == (2, 2, False)

        persistence_released.set()
        first = await asyncio.wait_for(passed_q.get(), 1)
        second = await asyncio.wait_for(passed_q.get(), 1)
        assert [
            (first.mint, first.safety_report_id),
            (second.mint, second.safety_report_id),
        ] == [("HELD", 101), ("NEXT", 102)]
        passed_q.task_done()
        passed_q.task_done()
        await asyncio.wait_for(runner._q.join(), 1)
        await asyncio.wait_for(idle_waiter, 1)

        assert gate.evaluations == ["HELD", "NEXT"]
        assert gate.persisted_drafts == [held_draft, held_draft, next_draft]
        assert bus.critical_state() == (2, 0, False)
    finally:
        persistence_released.set()
        stop.set()
        if idle_waiter is not None and not idle_waiter.done():
            idle_waiter.cancel()
            await asyncio.gather(idle_waiter, return_exceptions=True)
        if not task.done():
            await asyncio.wait_for(task, 2)
        else:
            await asyncio.gather(task, return_exceptions=True)
