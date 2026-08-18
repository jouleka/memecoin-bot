import asyncio
import socket
import tempfile
from contextlib import contextmanager
from pathlib import Path

from memebot.ops import Heartbeat, sd_notify


@contextmanager
def bound_notify_socket():
    # macOS limits AF_UNIX paths to 104 bytes; pytest's tmp_path can exceed that.
    with tempfile.TemporaryDirectory(prefix="mb-ops-", dir="/tmp") as directory:
        sock_path = str(Path(directory) / "notify.sock")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            server.bind(sock_path)
            server.settimeout(2)
            yield server, sock_path
        finally:
            server.close()


def test_sd_notify_no_socket_env_is_noop():
    assert sd_notify("READY=1", env={}) is False


def test_sd_notify_sends_datagram():
    with bound_notify_socket() as (server, sock_path):
        assert sd_notify("WATCHDOG=1", env={"NOTIFY_SOCKET": sock_path}) is True
        assert server.recv(64) == b"WATCHDOG=1"


async def test_heartbeat_touches_file_and_pets_watchdog(tmp_path):
    with bound_notify_socket() as (server, sock_path):
        hb_file = tmp_path / "heartbeat"
        stop = asyncio.Event()
        hb = Heartbeat(hb_file, interval=0.05, env={"NOTIFY_SOCKET": sock_path})
        task = asyncio.create_task(hb.run(stop))
        await asyncio.sleep(0.2)
        stop.set()
        await asyncio.wait_for(task, 2)
        assert hb_file.exists()
        assert server.recv(64) == b"WATCHDOG=1"


async def test_heartbeat_survives_notify_failure(tmp_path):
    hb_file = tmp_path / "heartbeat"
    stop = asyncio.Event()
    hb = Heartbeat(hb_file, interval=0.05,
                   env={"NOTIFY_SOCKET": str(tmp_path / "nobody-listening.sock")})
    task = asyncio.create_task(hb.run(stop))
    await asyncio.sleep(0.15)
    assert not task.done()   # loop survived failing sd_notify
    assert hb_file.exists()  # file touch still happened
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_heartbeat_survives_touch_failure(tmp_path):
    import shutil

    gone = tmp_path / "gone"
    gone.mkdir()
    hb_file = gone / "heartbeat"
    stop = asyncio.Event()
    hb = Heartbeat(hb_file, interval=0.05, env={})
    task = asyncio.create_task(hb.run(stop))
    await asyncio.sleep(0.08)
    shutil.rmtree(gone)      # parent dir vanishes mid-run
    await asyncio.sleep(0.15)
    assert not task.done()   # loop survived failing touch
    stop.set()
    await asyncio.wait_for(task, 2)
