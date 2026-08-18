"""Append-only JSONL event journal with size rotation + age retention (spec §5.1).

Sync buffered writes with flush-per-append: correctness first; optimize only if
profiling in a later milestone says so (YAGNI).

Durability boundary: flush-per-append survives process kill (systemd watchdog/
SIGKILL) via the OS page cache, but not OS crash/power loss — accepted for v1;
SQLite holds the authoritative ledger.
"""
from __future__ import annotations

import json
import math
import time
import types
from collections.abc import Callable, Iterator
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, get_args, get_origin, get_type_hints

from memebot.events import (
    EVENT_TYPES,
    CurveProgress,
    Event,
    LifecycleTransition,
    event_from_dict,
)


_MAX_UNIX_TS = 4_102_444_800.0


@dataclass(frozen=True, slots=True)
class JournalReplayGap:
    mint: str | None
    lower_wall: float
    upper_wall: float
    file_seq: int
    line_number: int


def _parse_stem(stem: str) -> tuple[int, int] | None:
    """Parse an ``events-<ts>-<seq>`` stem into (created_ts, seq), or None if foreign.

    Any file matching the glob but not fully numeric (stray autosave, half-written
    file) is treated as foreign: never counted for seq-resume, never pruned.
    """
    parts = stem.split("-")
    if len(parts) != 3:
        return None
    try:
        return int(parts[1]), int(parts[2])
    except ValueError:
        return None


def _is_wall(value: object) -> bool:
    return _is_finite_number(value) and 0.0 <= value <= _MAX_UNIX_TS


def _is_finite_number(value: object) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _is_mint(value: object) -> bool:
    return type(value) is str and bool(value.strip())


def _matches_annotation(value: object, annotation: object) -> bool:
    if annotation is Any:
        return True
    if annotation is float:
        return _is_finite_number(value)
    if annotation is int:
        return type(value) is int
    if annotation is str:
        return type(value) is str
    if annotation is type(None):
        return value is None

    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (types.UnionType,):
        return any(_matches_annotation(value, item) for item in args)
    if origin is dict:
        if type(value) is not dict:
            return False
        key_type, value_type = args
        return all(
            _matches_annotation(key, key_type)
            and _matches_annotation(item, value_type)
            for key, item in value.items()
        )
    if origin is tuple:
        if type(value) not in (list, tuple):
            return False
        if len(args) == 2 and args[1] is Ellipsis:
            return all(_matches_annotation(item, args[0]) for item in value)
        return len(value) == len(args) and all(
            _matches_annotation(item, item_type)
            for item, item_type in zip(value, args, strict=True)
        )
    return isinstance(value, annotation)


def _strict_decode(obj: dict[str, object]) -> Event:
    event = event_from_dict(obj)
    hints = get_type_hints(type(event))
    if any(
        not _matches_annotation(getattr(event, field.name), hints[field.name])
        for field in fields(event)
    ):
        raise ValueError("invalid registered event field type")
    if not _is_wall(event.t_wall):
        raise ValueError("invalid registered event wall time")
    if hasattr(event, "mint") and not _is_mint(getattr(event, "mint")):
        raise ValueError("invalid registered event mint")
    return event


def _load_line(line: bytes) -> dict[str, object]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant: {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON key: {key}")
            value[key] = item
        return value

    value = json.loads(
        line.decode("utf-8"),
        parse_constant=reject_constant,
        object_pairs_hook=reject_duplicate_keys,
    )
    if type(value) is not dict:
        raise ValueError("journal row must be an object")
    return value


def _decode_registered(obj: dict[str, object]) -> Event:
    kind = obj.get("kind")
    if type(kind) is not str or kind not in EVENT_TYPES:
        raise ValueError("missing or unknown registered event kind")
    return _strict_decode(obj)


class _ValidWallLookahead:
    def __init__(self, files: list[tuple[int, Path]]) -> None:
        self._lines = self._iter_lines(files)
        self._next_position: tuple[int, int] | None = None
        self._next_wall: float | None = None
        self._exhausted = False

    @staticmethod
    def _iter_lines(
        files: list[tuple[int, Path]],
    ) -> Iterator[tuple[tuple[int, int], bytes]]:
        for file_index, (_, path) in enumerate(files):
            with path.open("rb") as source:
                for line_number, line in enumerate(source, start=1):
                    yield (file_index, line_number), line

    def after(self, position: tuple[int, int]) -> float | None:
        if self._exhausted:
            return None
        if self._next_position is not None and self._next_position > position:
            return self._next_wall

        self._next_position = None
        self._next_wall = None
        for next_position, line in self._lines:
            if next_position <= position:
                continue
            try:
                event = _decode_registered(_load_line(line))
            except (
                KeyError,
                OverflowError,
                RecursionError,
                TypeError,
                UnicodeDecodeError,
                ValueError,
            ):
                continue
            self._next_position = next_position
            self._next_wall = float(event.t_wall)
            return self._next_wall

        self._exhausted = True
        return None


class Journal:
    def __init__(
        self,
        directory: Path,
        max_bytes: int,
        retention_days: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._dir = directory
        self._max_bytes = max_bytes
        self._retention_s = retention_days * 86400
        self._clock = clock
        self._seq = 0
        self._file = None
        self._bytes = 0
        self._dir.mkdir(parents=True, exist_ok=True)
        # Resume past the numeric-max seq (lexicographic-last is wrong with
        # unequal-length timestamps and could append into an existing file).
        parsed_files = [
            (parsed, f)
            for f in self._dir.glob("events-*.jsonl")
            if (parsed := _parse_stem(f.stem)) is not None
        ]
        seqs = [parsed[1] for parsed, _ in parsed_files]
        self._seq = max(seqs, default=0)

        # Reuse the latest under-cap file across boots instead of always
        # starting a fresh file (leaked a near-empty file every boot
        # otherwise). "Latest" = max numeric seq, which is also the file
        # _open_new() would have picked next; only reuse it if there's room
        # left under max_bytes, and only if we can trust its size (must be
        # one of our own numerically-parseable files, never a foreign one).
        # Never reuse a file whose embedded timestamp is already past
        # retention: prune keys on that timestamp, so fresh events appended
        # into a stale-named file would be deleted long before their time.
        latest = max(parsed_files, key=lambda pf: pf[0], default=None)
        if latest is not None:
            (latest_ts, latest_seq), latest_path = latest
            size = latest_path.stat().st_size
            if (
                latest_seq == self._seq
                and size < self._max_bytes
                and latest_ts >= self._clock() - self._retention_s
            ):
                self._file = open(latest_path, "a", encoding="utf-8")
                self._bytes = size
                return
        self._open_new()

    def _open_new(self) -> None:
        if self._file:
            self._file.close()
        self._seq += 1
        path = self._dir / f"events-{int(self._clock())}-{self._seq:06d}.jsonl"
        self._file = open(path, "a", encoding="utf-8")
        self._bytes = path.stat().st_size

    def append(self, obj: dict[str, Any]) -> None:
        line = json.dumps(obj, separators=(",", ":")) + "\n"
        encoded = len(line.encode())
        if self._bytes + encoded > self._max_bytes and self._bytes > 0:
            self._open_new()
        self._file.write(line)
        self._file.flush()
        self._bytes += encoded

    def _numeric_files(self) -> list[tuple[int, Path]]:
        files = [
            (parsed[1], path)
            for path in self._dir.glob("events-*.jsonl")
            if (parsed := _parse_stem(path.stem)) is not None
        ]
        return sorted(files, key=lambda item: (item[0], item[1].name))

    def iter_events(
        self,
        *,
        since_wall: float,
        until_wall: float,
    ) -> Iterator[dict[str, object] | JournalReplayGap]:
        if (
            not _is_wall(since_wall)
            or not _is_wall(until_wall)
            or since_wall > until_wall
        ):
            raise ValueError("invalid journal replay bounds")

        files = self._numeric_files()
        lookahead = _ValidWallLookahead(files)
        previous_valid_wall: float | None = None
        for file_index, (file_seq, path) in enumerate(files):
            with path.open("rb") as source:
                for line_number, line in enumerate(source, start=1):
                    obj: dict[str, object] | None = None
                    try:
                        obj = _load_line(line)
                        event = _decode_registered(obj)
                    except (
                        KeyError,
                        OverflowError,
                        RecursionError,
                        TypeError,
                        UnicodeDecodeError,
                        ValueError,
                    ):
                        wall = obj.get("t_wall") if obj is not None else None
                        mint = obj.get("mint") if obj is not None else None
                        if _is_wall(wall) and _is_mint(mint):
                            if since_wall <= wall <= until_wall:
                                yield JournalReplayGap(
                                    mint=mint,
                                    lower_wall=float(wall),
                                    upper_wall=float(wall),
                                    file_seq=file_seq,
                                    line_number=line_number,
                                )
                            continue

                        next_valid_wall = lookahead.after((file_index, line_number))
                        if previous_valid_wall is None or next_valid_wall is None:
                            lower_wall = float(since_wall)
                            upper_wall = float(until_wall)
                        else:
                            lower_wall = max(
                                float(since_wall),
                                min(previous_valid_wall, next_valid_wall),
                            )
                            upper_wall = min(
                                float(until_wall),
                                max(previous_valid_wall, next_valid_wall),
                            )
                            if lower_wall > upper_wall:
                                continue
                        yield JournalReplayGap(
                            mint=None,
                            lower_wall=lower_wall,
                            upper_wall=upper_wall,
                            file_seq=file_seq,
                            line_number=line_number,
                        )
                        continue

                    previous_valid_wall = float(event.t_wall)
                    if not since_wall <= event.t_wall <= until_wall:
                        continue
                    if isinstance(event, (CurveProgress, LifecycleTransition)):
                        yield obj

    def prune(self) -> list[Path]:
        """Delete journal files whose embedded timestamp is past retention.

        Never the live file; never a file whose name we can't fully parse.
        """
        cutoff = self._clock() - self._retention_s
        removed = []
        live = Path(self._file.name)
        for f in sorted(self._dir.glob("events-*.jsonl")):
            if f == live:
                continue
            parsed = _parse_stem(f.stem)
            if parsed is None:
                continue
            if parsed[0] < cutoff:
                f.unlink()
                removed.append(f)
        return removed

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None
