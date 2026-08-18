"""Asyncio event bus: journal-first publish, typed subscriptions, backpressure via
bounded queues (publisher awaits; a slow consumer slows the pipeline rather than
silently dropping — spec §5.1 journal completeness)."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TypeVar

from memebot.events import Event, event_to_dict

E = TypeVar("E", bound=Event)


@dataclass(slots=True)
class _Subscription:
    event_types: tuple[type[Event], ...]
    queue: asyncio.Queue[Event]
    critical: bool
    close_event: asyncio.Event
    closed: bool = False
    critical_outstanding: int = 0


class EventBus:
    def __init__(self, journal=None, maxsize: int = 10_000) -> None:
        self._journal = journal
        self._maxsize = maxsize
        self._subs: list[_Subscription] = []
        self._critical_epoch = 0
        self._critical_pending = 0
        self._critical_fatal = False
        self._critical_idle_or_failed = asyncio.Event()
        self._critical_idle_or_failed.set()

    def subscribe(
        self, *event_types: type[E], critical: bool = False,
    ) -> asyncio.Queue[E]:
        """Requires at least one event type; a no-arg call creates a queue that
        never receives anything."""
        queue: asyncio.Queue[E] = asyncio.Queue(maxsize=self._maxsize)
        self._subs.append(_Subscription(
            event_types=event_types,
            queue=queue,
            critical=critical,
            close_event=asyncio.Event(),
        ))
        return queue

    def unsubscribe(self, queue: asyncio.Queue[Event]) -> None:
        for subscription in self._subs:
            if subscription.queue is queue:
                subscription.closed = True
                subscription.close_event.set()
                self._subs.remove(subscription)
                if subscription.critical and subscription.critical_outstanding > 0:
                    self._critical_fatal = True
                    self._critical_idle_or_failed.set()
                return

    def critical_done(self, queue: asyncio.Queue[Event]) -> None:
        subscription = next(
            (
                subscription for subscription in self._subs
                if subscription.queue is queue
                and subscription.critical
                and subscription.critical_outstanding > 0
            ),
            None,
        )
        if subscription is None:
            self._critical_fatal = True
            self._critical_idle_or_failed.set()
            return

        queue.task_done()
        subscription.critical_outstanding -= 1
        self._critical_pending -= 1
        if self._critical_pending == 0:
            self._critical_idle_or_failed.set()

    def critical_state(self) -> tuple[int, int, bool]:
        return (
            self._critical_epoch,
            self._critical_pending,
            self._critical_fatal,
        )

    async def wait_critical_idle_or_failed(self) -> None:
        if self._critical_pending == 0 or self._critical_fatal:
            return
        await self._critical_idle_or_failed.wait()

    async def publish(self, event: Event) -> None:
        subscriptions = tuple(self._subs)
        if self._journal is not None:
            # sync file IO on the loop — acceptable at current rates; executor seam if
            # profiling says otherwise
            self._journal.append(event_to_dict(event))  # journal BEFORE fan-out
        for subscription in subscriptions:
            if isinstance(event, subscription.event_types):
                await self._put_unless_closed(subscription, event)

    async def _put_unless_closed(
        self, subscription: _Subscription, event: Event,
    ) -> None:
        if subscription.critical and subscription.closed:
            return

        if subscription.critical:
            self._critical_epoch += 1
            self._critical_pending += 1
            subscription.critical_outstanding += 1
            self._critical_idle_or_failed.clear()

        delivered = False
        put_task = asyncio.create_task(subscription.queue.put(event))
        close_task = asyncio.create_task(subscription.close_event.wait())
        try:
            await asyncio.wait(
                (put_task, close_task), return_when=asyncio.FIRST_COMPLETED,
            )
            if put_task.done():
                delivered = (
                    not put_task.cancelled() and put_task.exception() is None
                )
                await EventBus._cancel_and_await(close_task)
                await put_task
                return

            await EventBus._cancel_and_await(put_task)
            await close_task
        except BaseException:
            put_task.cancel()
            close_task.cancel()
            await asyncio.gather(put_task, close_task, return_exceptions=True)
            delivered = (
                not put_task.cancelled() and put_task.exception() is None
            )
            raise
        finally:
            if subscription.critical and not delivered:
                subscription.critical_outstanding -= 1
                self._critical_pending -= 1
                if self._critical_pending == 0:
                    self._critical_idle_or_failed.set()

    @staticmethod
    async def _cancel_and_await(task: asyncio.Task) -> None:
        task.cancel()
        result = (await asyncio.gather(task, return_exceptions=True))[0]
        if isinstance(result, BaseException) and not isinstance(
            result, asyncio.CancelledError,
        ):
            raise result
