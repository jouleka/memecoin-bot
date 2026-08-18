"""Ops primitives: minimal sd_notify (no dependency) + heartbeat loop.

Heartbeat touches a file (fate-isolated liveness signal, polymarket S4 pattern)
and pets the systemd watchdog. systemd Type=notify + WatchdogSec restarts us if
the loop wedges (spec §5.10).
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Mapping
from pathlib import Path


def sd_notify(message: str, env: Mapping[str, str] | None = None) -> bool:
    env = os.environ if env is None else env
    target = env.get("NOTIFY_SOCKET")
    if not target:
        return False
    if target.startswith("@"):
        target = "\0" + target[1:]
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
        s.connect(target)
        s.send(message.encode())
    return True


class Heartbeat:
    def __init__(self, path: Path, interval: float, env: Mapping[str, str] | None = None) -> None:
        self._path = path
        self._interval = interval
        self._env = env

    def beat(self) -> None:
        # Only OSError is caught: a transient IO blip on the petting mechanism
        # must not kill the heartbeat task (systemd would then SIGKILL a healthy
        # process ~WatchdogSec later). A genuinely wedged loop still dies loud.
        try:
            self._path.touch()
        except OSError:
            logging.getLogger(__name__).exception("heartbeat: touch failed")
        try:
            sd_notify("WATCHDOG=1", env=self._env)
        except OSError:
            logging.getLogger(__name__).exception("heartbeat: sd_notify failed")

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.beat()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
            except TimeoutError:
                continue
