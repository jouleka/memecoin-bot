import json

import memebot.journal as journal_module
from memebot.journal import Journal, JournalReplayGap


def read_all_lines(dirpath):
    lines = []
    for f in sorted(dirpath.glob("events-*.jsonl")):
        lines += [json.loads(x) for x in f.read_text().splitlines()]
    return lines


def test_append_and_byte_faithful_readback(tmp_path):
    j = Journal(tmp_path, max_bytes=10_000, retention_days=30)
    j.append({"kind": "adapter_health", "adapter": "t", "n": 1})
    j.append({"kind": "adapter_health", "adapter": "t", "n": 2})
    j.close()
    lines = read_all_lines(tmp_path)
    assert [x["n"] for x in lines] == [1, 2]


def test_rotation_at_max_bytes(tmp_path):
    j = Journal(tmp_path, max_bytes=120, retention_days=30)
    for n in range(10):
        j.append({"kind": "k", "n": n, "pad": "x" * 40})
    j.close()
    files = list(tmp_path.glob("events-*.jsonl"))
    assert len(files) > 1                      # rotated
    assert len(read_all_lines(tmp_path)) == 10 # nothing lost


def test_retention_prunes_old_files(tmp_path):
    now = [1_000_000.0]
    j = Journal(tmp_path, max_bytes=50, retention_days=1, clock=lambda: now[0])
    j.append({"n": 1, "pad": "x" * 60})   # forces file 1 full
    j.append({"n": 2, "pad": "x" * 60})   # rotates to file 2
    now[0] += 2 * 86400                   # 2 days later
    j.append({"n": 3})
    removed = j.prune()
    j.close()
    assert len(removed) >= 1
    remaining = [x["n"] for x in read_all_lines(tmp_path)]
    assert 3 in remaining


def test_seq_resume_uses_numeric_max(tmp_path):
    # lexicographic sort would pick events-999999-000005 (9 > 1 as strings) and resume seq=6,
    # eventually colliding with the existing seq-50 file; numeric max must pick 50.
    #
    # NOTE (deliberate semantic change, MB-3 adversarial-review M2): the journal now
    # reuses the newest under-cap file across restarts instead of always opening a
    # fresh file at boot. So the seq-50 file (numeric max, and newest by embedded ts)
    # is no longer "untouched" — the n=51 append lands in IT, ordered after n=50.
    # This still kills the original lexicographic-resume mutant: that mutant would
    # resume at seq=6 and either collide with / corrupt the seq-5 file or otherwise
    # diverge from "n=51 appended after n=50 in the seq-50 file, seq-5 untouched,
    # and the next rotation starts at seq 51".
    (tmp_path / "events-1000000-000050.jsonl").write_text('{"n": 50}\n')
    (tmp_path / "events-999999-000005.jsonl").write_text('{"n": 5}\n')
    j = Journal(tmp_path, max_bytes=10_000, retention_days=30, clock=lambda: 1_000_001.0)
    j.append({"n": 51})
    j.close()
    assert (tmp_path / "events-999999-000005.jsonl").read_text() == '{"n": 5}\n'  # untouched
    assert (tmp_path / "events-1000000-000050.jsonl").read_text() == (
        '{"n": 50}\n{"n":51}\n'
    )  # n=51 appended after n=50 (append() writes compact JSON), same (reused) file
    assert not (tmp_path / "events-1000001-000051.jsonl").exists()  # no new file yet

    # A subsequent rotation must still start numbering past the reused file's seq.
    j2 = Journal(tmp_path, max_bytes=10_000, retention_days=30, clock=lambda: 1_000_002.0)
    j2._open_new()  # force rotation to prove next-seq bookkeeping
    j2.close()
    assert (tmp_path / "events-1000002-000051.jsonl").exists()


def test_foreign_files_ignored_by_init_and_prune(tmp_path):
    (tmp_path / "events-notanumber-000001.jsonl").write_text("junk\n")
    (tmp_path / "events-1000000-abcxyz.jsonl").write_text("junk\n")
    now = [1_000_000.0 + 10 * 86400]
    j = Journal(tmp_path, max_bytes=10_000, retention_days=1, clock=lambda: now[0])
    j.append({"n": 1})
    removed = j.prune()
    j.close()
    assert (tmp_path / "events-notanumber-000001.jsonl").exists()   # never deleted
    assert (tmp_path / "events-1000000-abcxyz.jsonl").exists()      # never deleted
    assert all("notanumber" not in p.name and "abcxyz" not in p.name for p in removed)


def test_reopen_reuses_latest_file_under_cap(tmp_path):
    j1 = Journal(tmp_path, max_bytes=10_000, retention_days=30)
    j1.append({"n": 1})
    j1.close()
    j2 = Journal(tmp_path, max_bytes=10_000, retention_days=30)
    j2.append({"n": 2})
    j2.close()
    files = list(tmp_path.glob("events-*.jsonl"))
    assert len(files) == 1  # second boot appended to the same under-cap file


def test_reopen_rotates_when_latest_is_full(tmp_path):
    j1 = Journal(tmp_path, max_bytes=30, retention_days=30)
    j1.append({"n": 1, "pad": "x" * 40})  # single oversize line fills file 1
    j1.close()
    j2 = Journal(tmp_path, max_bytes=30, retention_days=30)
    j2.append({"n": 2})
    j2.close()
    assert len(list(tmp_path.glob("events-*.jsonl"))) == 2


def test_reuse_skipped_for_files_older_than_retention(tmp_path):
    # Reusing a stale-named under-cap file would append fresh events into a file
    # whose embedded timestamp is already past retention — prune (keyed on that
    # timestamp) would then delete the fresh events long before their 30 days.
    old_ts = 1_000_000
    (tmp_path / f"events-{old_ts}-000001.jsonl").write_text('{"n": 1}\n')
    now = old_ts + 40 * 86400  # 40 days later, retention 30
    j = Journal(tmp_path, max_bytes=10_000, retention_days=30, clock=lambda: float(now))
    j.append({"n": 2})
    j.close()
    files = sorted(p.name for p in tmp_path.glob("events-*.jsonl"))
    assert len(files) == 2                                # did NOT reuse the stale file
    assert files[0] == f"events-{old_ts}-000001.jsonl"    # old file untouched
    assert (tmp_path / files[0]).read_text() == '{"n": 1}\n'


def test_iter_events_poisoning_gap_contract(tmp_path, monkeypatch):
    (tmp_path / "events-200-000010.jsonl").write_text(
        "\n".join(
            (
                json.dumps({
                    "kind": "lifecycle_transition",
                    "t_wall": 14.0,
                    "t_mono": 4.0,
                    "mint": "mint-b",
                    "from_state": "FRESH",
                    "to_state": "CLIMBING",
                }),
                json.dumps({
                    "kind": "curve_progress",
                    "t_wall": 30.0,
                    "t_mono": 5.0,
                    "mint": "outside",
                    "progress_pct": 30.0,
                }),
                "{tail-not-json",
                "[]",
            )
        ) + "\n"
    )
    replay_prefix = "\n".join(
        (
            json.dumps({
                "kind": "curve_progress",
                "t_wall": 10.0,
                "t_mono": 1.0,
                "mint": "mint-a",
                "progress_pct": 10.0,
            }),
            json.dumps({
                "kind": "adapter_health",
                "t_wall": 11.0,
                "t_mono": 2.0,
                "adapter": "pumpportal",
                "status": "up",
                "detail": "ok",
            }),
            json.dumps({
                "kind": "curve_progress",
                "t_wall": 12.0,
                "t_mono": 3.0,
                "mint": "mint-a",
            }),
            json.dumps({
                "kind": "unregistered_event",
                "t_wall": 13.0,
                "mint": "mint-b",
            }),
            "{not-json",
            "[]",
            json.dumps({"mint": "missing-kind-and-time"}),
            json.dumps({"kind": None}),
            "{also-not-json",
            json.dumps({
                "kind": "curve_progress",
                "t_wall": 13.5,
                "t_mono": 3.5,
                "mint": "mint-a",
                "progress_pct": 10**1000,
            }),
        )
    ) + "\n"
    (tmp_path / "events-100-000002.jsonl").write_bytes(
        replay_prefix.encode("utf-8")
        + b"\xff\n"
        + ("[" * 2000 + "0" + "]" * 2000 + "\n").encode("utf-8")
        + (
            '{"kind":"curve_progress","t_wall":13.7,"t_wall":13.8,'
            '"t_mono":3.8,"mint":"mint-a","progress_pct":13.8}\n'
        ).encode("utf-8")
    )

    load_calls = 0
    original_load_line = journal_module._load_line

    def counted_load_line(line):
        nonlocal load_calls
        load_calls += 1
        return original_load_line(line)

    monkeypatch.setattr(journal_module, "_load_line", counted_load_line)

    journal = Journal(
        tmp_path,
        max_bytes=10_000,
        retention_days=30,
        clock=lambda: 200.0,
    )
    try:
        items = list(journal.iter_events(since_wall=9.0, until_wall=20.0))
    finally:
        journal.close()

    assert items == [
        {
            "kind": "curve_progress",
            "t_wall": 10.0,
            "t_mono": 1.0,
            "mint": "mint-a",
            "progress_pct": 10.0,
        },
        JournalReplayGap(
            mint="mint-a",
            lower_wall=12.0,
            upper_wall=12.0,
            file_seq=2,
            line_number=3,
        ),
        JournalReplayGap(
            mint="mint-b",
            lower_wall=13.0,
            upper_wall=13.0,
            file_seq=2,
            line_number=4,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=11.0,
            upper_wall=14.0,
            file_seq=2,
            line_number=5,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=11.0,
            upper_wall=14.0,
            file_seq=2,
            line_number=6,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=11.0,
            upper_wall=14.0,
            file_seq=2,
            line_number=7,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=11.0,
            upper_wall=14.0,
            file_seq=2,
            line_number=8,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=11.0,
            upper_wall=14.0,
            file_seq=2,
            line_number=9,
        ),
        JournalReplayGap(
            mint="mint-a",
            lower_wall=13.5,
            upper_wall=13.5,
            file_seq=2,
            line_number=10,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=11.0,
            upper_wall=14.0,
            file_seq=2,
            line_number=11,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=11.0,
            upper_wall=14.0,
            file_seq=2,
            line_number=12,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=11.0,
            upper_wall=14.0,
            file_seq=2,
            line_number=13,
        ),
        {
            "kind": "lifecycle_transition",
            "t_wall": 14.0,
            "t_mono": 4.0,
            "mint": "mint-b",
            "from_state": "FRESH",
            "to_state": "CLIMBING",
        },
        JournalReplayGap(
            mint=None,
            lower_wall=9.0,
            upper_wall=20.0,
            file_seq=10,
            line_number=3,
        ),
        JournalReplayGap(
            mint=None,
            lower_wall=9.0,
            upper_wall=20.0,
            file_seq=10,
            line_number=4,
        ),
    ]
    assert load_calls == 27
