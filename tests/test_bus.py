import asyncio

import pytest

from memebot.bus import EventBus
from memebot.events import AdapterHealth, MarketRegime


def health(n="a"):
    return AdapterHealth(t_wall=1.0, t_mono=2.0, adapter=n, status="up", detail="")


class FakeJournal:
    def __init__(self):
        self.lines = []

    def append(self, obj):
        self.lines.append(obj)


async def test_delivers_to_matching_subscribers_only():
    bus = EventBus()
    q_health = bus.subscribe(AdapterHealth)
    q_regime = bus.subscribe(MarketRegime)
    await bus.publish(health())
    assert (await asyncio.wait_for(q_health.get(), 1)).adapter == "a"
    assert q_regime.empty()


async def test_journal_written_even_with_no_subscribers():
    j = FakeJournal()
    bus = EventBus(journal=j)
    await bus.publish(health())
    assert j.lines and j.lines[0]["kind"] == "adapter_health"


async def test_multi_subscriber_fanout():
    bus = EventBus()
    q1 = bus.subscribe(AdapterHealth)
    q2 = bus.subscribe(AdapterHealth)
    await bus.publish(health())
    assert not q1.empty() and not q2.empty()


async def test_subscription_metadata_and_idempotent_unsubscribe():
    bus = EventBus()
    queue = bus.subscribe(AdapterHealth, critical=True)
    subscription = bus._subs[0]
    ordinary = bus.subscribe(AdapterHealth)
    ordinary_subscription = bus._subs[1]

    assert subscription.event_types == (AdapterHealth,)
    assert subscription.queue is queue
    assert subscription.critical is True
    assert subscription.closed is False
    assert subscription.close_event.is_set() is False
    assert ordinary_subscription.queue is ordinary
    assert ordinary_subscription.critical is False
    assert ordinary_subscription.close_event is not subscription.close_event

    await bus.publish(health("queued"))
    bus.unsubscribe(queue)

    assert subscription.closed is True
    assert subscription.close_event.is_set() is True
    assert subscription not in bus._subs
    assert (await queue.get()).adapter == "queued"
    assert (await ordinary.get()).adapter == "queued"

    bus.unsubscribe(queue)
    bus.unsubscribe(asyncio.Queue())
    await bus.publish(health("after-close"))
    assert queue.empty()
    assert (await ordinary.get()).adapter == "after-close"
    assert ordinary_subscription in bus._subs
    assert ordinary_subscription.closed is False
    assert ordinary_subscription.close_event.is_set() is False


async def test_critical_delivery_epoch_pending_and_ack_contract(monkeypatch):
    bus = EventBus()
    first = bus.subscribe(AdapterHealth, critical=True)
    second = bus.subscribe(AdapterHealth, critical=True)
    ordinary = bus.subscribe(AdapterHealth)

    assert bus.critical_state() == (0, 0, False)
    await asyncio.wait_for(bus.wait_critical_idle_or_failed(), 1)

    await bus.publish(health("held"))
    assert bus.critical_state() == (2, 2, False)
    assert (await first.get()).adapter == "held"
    assert (await second.get()).adapter == "held"
    assert (await ordinary.get()).adapter == "held"
    assert bus.critical_state() == (2, 2, False)

    waiter = asyncio.create_task(bus.wait_critical_idle_or_failed())
    await asyncio.sleep(0)
    assert waiter.done() is False

    first_task_done_calls = 0
    original_first_task_done = first.task_done

    def observed_first_task_done():
        nonlocal first_task_done_calls
        first_task_done_calls += 1
        original_first_task_done()

    first.task_done = observed_first_task_done
    bus.critical_done(first)
    assert bus.critical_state() == (2, 1, False)
    assert first_task_done_calls == 1
    await asyncio.wait_for(first.join(), 1)
    assert waiter.done() is False

    bus.critical_done(second)
    assert bus.critical_state() == (2, 0, False)
    await asyncio.wait_for(second.join(), 1)
    await asyncio.wait_for(waiter, 1)

    cancelled_bus = EventBus()
    cancelled_queue = cancelled_bus.subscribe(AdapterHealth, critical=True)
    put_completed = asyncio.Event()
    hold_wait_result = asyncio.Event()
    original_wait = asyncio.wait

    async def paused_wait(tasks, *, return_when):
        result = await original_wait(tasks, return_when=return_when)
        put_completed.set()
        await hold_wait_result.wait()
        return result

    monkeypatch.setattr(asyncio, "wait", paused_wait)
    cancelled_publisher = asyncio.create_task(
        cancelled_bus.publish(health("cancelled-after-put")),
    )
    await asyncio.wait_for(put_completed.wait(), 1)
    cancelled_publisher.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(cancelled_publisher, 1)
    monkeypatch.setattr(asyncio, "wait", original_wait)

    assert cancelled_bus.critical_state() == (1, 1, False)
    assert (await cancelled_queue.get()).adapter == "cancelled-after-put"
    cancelled_bus.critical_done(cancelled_queue)
    assert cancelled_bus.critical_state() == (1, 0, False)

    wrong_bus = EventBus()
    wrong_held = wrong_bus.subscribe(AdapterHealth, critical=True)
    await wrong_bus.publish(health("wrong"))
    await wrong_held.get()
    wrong_waiter = asyncio.create_task(
        wrong_bus.wait_critical_idle_or_failed(),
    )
    await asyncio.sleep(0)
    assert wrong_waiter.done() is False

    wrong = asyncio.Queue()
    wrong_task_done_calls = 0
    original_wrong_task_done = wrong.task_done

    def observed_wrong_task_done():
        nonlocal wrong_task_done_calls
        wrong_task_done_calls += 1
        original_wrong_task_done()

    wrong.task_done = observed_wrong_task_done
    wrong_bus.critical_done(wrong)
    assert wrong_bus.critical_state() == (1, 1, True)
    assert wrong_task_done_calls == 0
    await asyncio.wait_for(wrong_waiter, 1)
    wrong_bus.critical_done(wrong_held)
    assert wrong_bus.critical_state() == (1, 0, True)

    noncritical_bus = EventBus()
    noncritical_held = noncritical_bus.subscribe(AdapterHealth, critical=True)
    noncritical = noncritical_bus.subscribe(AdapterHealth)
    await noncritical_bus.publish(health("noncritical"))
    await noncritical_held.get()
    await noncritical.get()
    assert noncritical_bus.critical_state() == (1, 1, False)
    noncritical_waiter = asyncio.create_task(
        noncritical_bus.wait_critical_idle_or_failed(),
    )
    await asyncio.sleep(0)
    assert noncritical_waiter.done() is False

    noncritical_task_done_calls = 0
    original_noncritical_task_done = noncritical.task_done

    def observed_noncritical_task_done():
        nonlocal noncritical_task_done_calls
        noncritical_task_done_calls += 1
        original_noncritical_task_done()

    noncritical.task_done = observed_noncritical_task_done
    noncritical_bus.critical_done(noncritical)
    assert noncritical_bus.critical_state() == (1, 1, True)
    assert noncritical_task_done_calls == 0
    await asyncio.wait_for(noncritical_waiter, 1)
    noncritical_bus.critical_done(noncritical_held)
    assert noncritical_bus.critical_state() == (1, 0, True)

    underflow_bus = EventBus()
    underflow = underflow_bus.subscribe(MarketRegime, critical=True)
    underflow_held = underflow_bus.subscribe(AdapterHealth, critical=True)
    await underflow_bus.publish(health("underflow"))
    await underflow_held.get()
    assert underflow_bus.critical_state() == (1, 1, False)
    underflow_waiter = asyncio.create_task(
        underflow_bus.wait_critical_idle_or_failed(),
    )
    await asyncio.sleep(0)
    assert underflow_waiter.done() is False

    underflow_task_done_calls = 0
    original_underflow_task_done = underflow.task_done

    def observed_underflow_task_done():
        nonlocal underflow_task_done_calls
        underflow_task_done_calls += 1
        original_underflow_task_done()

    underflow.task_done = observed_underflow_task_done
    underflow_bus.critical_done(underflow)
    assert underflow_bus.critical_state() == (1, 1, True)
    assert underflow_task_done_calls == 0
    await asyncio.wait_for(underflow_waiter, 1)
    underflow_bus.critical_done(underflow_held)
    assert underflow_bus.critical_state() == (1, 0, True)


async def test_critical_unsubscribe_with_pending_releases_publisher_and_marks_fatal():
    clean_bus = EventBus()
    clean = clean_bus.subscribe(AdapterHealth, critical=True)
    clean_subscription = clean_bus._subs[0]
    clean_bus.unsubscribe(clean)

    assert clean_subscription.closed is True
    assert clean_subscription.close_event.is_set() is True
    assert clean_subscription not in clean_bus._subs
    assert clean_bus.critical_state() == (0, 0, False)
    await asyncio.wait_for(clean_bus.wait_critical_idle_or_failed(), 1)

    sequential_bus = EventBus(maxsize=1)
    earlier = sequential_bus.subscribe(AdapterHealth)
    later = sequential_bus.subscribe(AdapterHealth, critical=True)
    await earlier.put(health("earlier-full"))
    earlier_put_started = asyncio.Event()
    original_earlier_put = earlier.put

    async def observed_earlier_put(event):
        earlier_put_started.set()
        await original_earlier_put(event)

    earlier.put = observed_earlier_put
    sequential_publisher = asyncio.create_task(
        sequential_bus.publish(health("sequential")),
    )
    await asyncio.wait_for(earlier_put_started.wait(), 1)
    sequential_bus.unsubscribe(later)
    assert sequential_bus.critical_state() == (0, 0, False)

    assert (await earlier.get()).adapter == "earlier-full"
    await asyncio.wait_for(sequential_publisher, 1)
    assert (await earlier.get()).adapter == "sequential"
    assert later.empty()
    assert sequential_bus.critical_state() == (0, 0, False)

    noncritical_bus = EventBus(maxsize=1)
    noncritical_earlier = noncritical_bus.subscribe(AdapterHealth)
    noncritical_later = noncritical_bus.subscribe(AdapterHealth)
    await noncritical_earlier.put(health("noncritical-earlier-full"))
    noncritical_put_started = asyncio.Event()
    original_noncritical_put = noncritical_earlier.put

    async def observed_noncritical_put(event):
        noncritical_put_started.set()
        await original_noncritical_put(event)

    noncritical_earlier.put = observed_noncritical_put
    noncritical_publisher = asyncio.create_task(
        noncritical_bus.publish(health("noncritical-sequential")),
    )
    await asyncio.wait_for(noncritical_put_started.wait(), 1)
    noncritical_bus.unsubscribe(noncritical_later)

    assert (
        await noncritical_earlier.get()
    ).adapter == "noncritical-earlier-full"
    await asyncio.wait_for(noncritical_publisher, 1)
    assert (await noncritical_earlier.get()).adapter == "noncritical-sequential"
    assert (
        await asyncio.wait_for(noncritical_later.get(), 1)
    ).adapter == "noncritical-sequential"

    held_bus = EventBus()
    held = held_bus.subscribe(AdapterHealth, critical=True)
    held_subscription = held_bus._subs[0]
    await held_bus.publish(health("held"))
    assert (await held.get()).adapter == "held"

    task_done_calls = 0
    original_task_done = held.task_done

    def observed_task_done():
        nonlocal task_done_calls
        task_done_calls += 1
        original_task_done()

    held.task_done = observed_task_done
    held_waiter = asyncio.create_task(
        held_bus.wait_critical_idle_or_failed(),
    )
    await asyncio.sleep(0)
    assert held_waiter.done() is False

    held_bus.unsubscribe(held)

    assert held_subscription.closed is True
    assert held_subscription.close_event.is_set() is True
    assert held_subscription not in held_bus._subs
    assert held_bus.critical_state() == (1, 1, True)
    assert task_done_calls == 0
    assert held_subscription.critical_outstanding == 1
    await asyncio.wait_for(held_waiter, 1)

    held_bus.unsubscribe(held)
    assert held_bus.critical_state() == (1, 1, True)
    assert task_done_calls == 0

    blocked_bus = EventBus(maxsize=1)
    blocked = blocked_bus.subscribe(AdapterHealth, critical=True)
    await blocked.put(health("already-queued"))
    put_started = asyncio.Event()
    original_put = blocked.put

    async def observed_put(event):
        put_started.set()
        await original_put(event)

    blocked.put = observed_put
    publisher = asyncio.create_task(blocked_bus.publish(health("blocked")))
    await asyncio.wait_for(put_started.wait(), 1)
    assert blocked_bus.critical_state() == (1, 1, False)
    assert publisher.done() is False

    blocked_waiter = asyncio.create_task(
        blocked_bus.wait_critical_idle_or_failed(),
    )
    await asyncio.sleep(0)
    assert blocked_waiter.done() is False
    blocked_bus.unsubscribe(blocked)

    await asyncio.wait_for(blocked_waiter, 1)
    await asyncio.wait_for(publisher, 1)
    assert blocked_bus.critical_state() == (1, 0, True)
    assert (await blocked.get()).adapter == "already-queued"
    assert blocked.empty()


async def test_publish_full_queue_unsubscribe_race_cancels_put():
    bus = EventBus(maxsize=1)
    active = bus.subscribe(AdapterHealth)
    closing = bus.subscribe(AdapterHealth)
    await closing.put(health("already-queued"))

    closing_put_started = asyncio.Event()
    original_closing_put = closing.put

    async def observed_closing_put(event):
        closing_put_started.set()
        await original_closing_put(event)

    closing.put = observed_closing_put
    publisher = asyncio.create_task(bus.publish(health("race")))
    await asyncio.wait_for(closing_put_started.wait(), 1)
    assert publisher.done() is False
    assert (await active.get()).adapter == "race"

    bus.unsubscribe(closing)
    await asyncio.wait_for(publisher, 1)

    assert (await closing.get()).adapter == "already-queued"
    assert closing.empty()

    await bus.publish(health("active-full"))
    active_put_started = asyncio.Event()
    original_active_put = active.put

    async def observed_active_put(event):
        active_put_started.set()
        await original_active_put(event)

    active.put = observed_active_put
    backpressured = asyncio.create_task(bus.publish(health("active-waits")))
    await asyncio.wait_for(active_put_started.wait(), 1)
    assert backpressured.done() is False
    assert (await active.get()).adapter == "active-full"
    await asyncio.wait_for(backpressured, 1)
    assert (await active.get()).adapter == "active-waits"

    class SlowCancelEvent(asyncio.Event):
        def __init__(self):
            super().__init__()
            self.cleanup_started = asyncio.Event()
            self.cleanup_release = asyncio.Event()

        async def wait(self):
            try:
                return await super().wait()
            finally:
                self.cleanup_started.set()
                await self.cleanup_release.wait()

    put_won_bus = EventBus(maxsize=1)
    put_won_bus.subscribe(AdapterHealth)
    slow_close = SlowCancelEvent()
    put_won_bus._subs[0].close_event = slow_close
    put_won_publisher = asyncio.create_task(put_won_bus.publish(health("put-won")))
    await asyncio.wait_for(slow_close.cleanup_started.wait(), 1)
    put_won_publisher.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(put_won_publisher, 1)

    close_won_bus = EventBus(maxsize=1)
    close_won_queue = close_won_bus.subscribe(AdapterHealth)
    await close_won_queue.put(health("already-queued"))
    close_won_put_started = asyncio.Event()
    close_won_cleanup_started = asyncio.Event()
    close_won_cleanup_release = asyncio.Event()
    original_close_won_put = close_won_queue.put

    async def slow_cancel_put(event):
        close_won_put_started.set()
        try:
            await original_close_won_put(event)
        finally:
            close_won_cleanup_started.set()
            await close_won_cleanup_release.wait()

    close_won_queue.put = slow_cancel_put
    close_won_publisher = asyncio.create_task(
        close_won_bus.publish(health("close-won")),
    )
    await asyncio.wait_for(close_won_put_started.wait(), 1)
    close_won_bus.unsubscribe(close_won_queue)
    await asyncio.wait_for(close_won_cleanup_started.wait(), 1)
    close_won_publisher.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(close_won_publisher, 1)
    assert (await close_won_queue.get()).adapter == "already-queued"

    put_won_failure_bus = EventBus()
    put_won_failure_bus.subscribe(AdapterHealth)

    class FailingCloseEvent:
        async def wait(self):
            raise RuntimeError("close waiter failed")

    put_won_failure_bus._subs[0].close_event = FailingCloseEvent()
    with pytest.raises(RuntimeError, match="close waiter failed"):
        await put_won_failure_bus.publish(health("put-won-failure"))

    close_won_failure_bus = EventBus(maxsize=1)
    close_won_failure_queue = close_won_failure_bus.subscribe(AdapterHealth)
    await close_won_failure_queue.put(health("already-queued"))
    close_won_failure_started = asyncio.Event()
    original_close_won_failure_put = close_won_failure_queue.put

    async def failing_cancel_put(event):
        close_won_failure_started.set()
        try:
            await original_close_won_failure_put(event)
        except asyncio.CancelledError as exc:
            raise RuntimeError("put cleanup failed") from exc

    close_won_failure_queue.put = failing_cancel_put
    close_won_failure_publisher = asyncio.create_task(
        close_won_failure_bus.publish(health("close-won-failure")),
    )
    await asyncio.wait_for(close_won_failure_started.wait(), 1)
    close_won_failure_bus.unsubscribe(close_won_failure_queue)
    with pytest.raises(RuntimeError, match="put cleanup failed"):
        await asyncio.wait_for(close_won_failure_publisher, 1)
