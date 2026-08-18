"""Entrypoint: load config → open store → journal+bus → heartbeat → idle until stopped.

M1 runs no adapters; it proves the skeleton boots, journals, heartbeats and shuts
down cleanly. M2 registers stream adapters onto the same bus.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import secrets
import signal
import time
from pathlib import Path

import httpx

from memebot.bus import EventBus
from memebot.config import (
    load_config,
    validate_runtime_config,
    validate_watch_only_release,
)
from memebot.events import AdapterHealth, CandidateScored, SafetyHardFail
from memebot.ingest.curvepoller import CurvePoller
from memebot.ingest.pumpportal import PumpPortalStream
from memebot.ingest.supervisor import supervise
from memebot.journal import Journal
from memebot.lifecycle import LifecycleTracker
from memebot.logging_setup import setup_logging
from memebot.ops import Heartbeat, sd_notify
from memebot.safety.gate import GateRunner, LiveProbes, SafetyGate
from memebot.safety.governor import Governor
from memebot.store import (
    allocate_p3_causal_wall,
    assert_no_open_p3_positions,
    assert_p3_buy_terminal_coverage,
    mark_clean_shutdown,
    open_db,
    reconcile_unmatched_p3_buys,
    record_boot,
)
from memebot.telegram import HttpTransport, NullOps, TelegramOps

log = logging.getLogger("memebot.main")


async def _periodic_prune(journal, interval_s: float, stop: asyncio.Event) -> None:
    """Enforce journal retention mid-run, not just at boot.

    Boot-only pruning left a continuous run to cross disk_cap_bytes at M2 event
    rates (~51 days) since 30-day retention was never re-checked. journal.prune()
    is synchronous with no await inside — safe to call here alongside the bus's
    own synchronous journal.append() since both run on the same event loop
    (true concurrency would require a second OS thread, which this doesn't use).
    """
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except TimeoutError:
            removed = journal.prune()
            if removed:
                log.info("journal pruned", extra={"extra_fields": {"files_removed": len(removed)}})


async def _telegram_ops_loop(bus, ops, stop: asyncio.Event, *,
                             alert_on_rug: bool = False) -> None:
    """Forwards AdapterHealth (down/stale) to telegram and polls /status on the idle tick.

    SafetyHardFail (rug detections) are still CONSUMED here to keep the bus drained (the
    C2-review note: a stuck consumer backpressures the whole bus), but are NOT alerted by
    default. On pump.fun the large majority of tokens are genuine rugs, so a per-rug ping is
    pure noise -- the owner wants telegram reserved for the bot's own buy/sell decisions
    (M4+), not rug spam. Rug verdicts stay internal: they gate the bot's entry/exit logic
    and persist to safety_reports. Flip [telegram].alert_on_rug=true only for live debugging.

    ops.alert/poll_once themselves swallow transport errors (TelegramOps), so this loop's
    try/except only guards a bug in this loop's own logic, not transport failures.
    """
    q = bus.subscribe(AdapterHealth, SafetyHardFail)
    try:
        while not stop.is_set():
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except TimeoutError:
                await ops.poll_once()      # /status on the idle tick
                continue
            try:
                if isinstance(ev, AdapterHealth) and ev.status in ("down", "stale"):
                    await ops.alert(f"adapter {ev.adapter} {ev.status}: {ev.detail}")
                elif isinstance(ev, SafetyHardFail) and alert_on_rug:
                    await ops.alert(f"RUG {ev.mint}: {','.join(ev.reasons)}")
            except Exception:
                log.exception("telegram ops loop failed to forward event")
    finally:
        bus.unsubscribe(q)


async def _trade_alert_loop(bus, ops, stop: asyncio.Event) -> None:
    """Forward PaperEntry/PaperExit to Telegram as BUY/SELL alerts. Kept separate from the
    strategy (which stays telegram-free) — mirrors the gate/telegram separation."""
    from memebot.events import PaperEntry, PaperExit
    from memebot.telegram import format_buy_alert, format_sell_alert
    q = bus.subscribe(PaperEntry, PaperExit)
    try:
        while not stop.is_set():
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except TimeoutError:
                continue
            try:
                if isinstance(ev, PaperEntry):
                    await ops.alert(format_buy_alert(ev))
                else:
                    await ops.alert(format_sell_alert(ev))
            except Exception:
                log.exception("trade alert loop failed to forward event")
    finally:
        bus.unsubscribe(q)


async def _watch_alert_loop(bus, ops, stop: asyncio.Event, *, queue=None) -> None:
    """Forward CandidateScored through the informational-only WATCH path."""
    from memebot.events import CandidateScored
    from memebot.telegram import format_watch_alert
    q = queue if queue is not None else bus.subscribe(CandidateScored)
    try:
        while not stop.is_set():
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except TimeoutError:
                continue

            async def deliver_watch() -> BaseException | None:
                try:
                    await ops.watch(
                        format_watch_alert(ev), mint=ev.mint, segment=ev.segment,
                    )
                except BaseException as exc:
                    return exc
                return None

            delivery_task = asyncio.create_task(
                deliver_watch(),
            )
            stop_task = asyncio.create_task(stop.wait())
            try:
                done, _ = await asyncio.wait(
                    (delivery_task, stop_task), return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    delivery_task.cancel()
                    result = (await asyncio.gather(
                        delivery_task, return_exceptions=True,
                    ))[0]
                    if isinstance(result, BaseException) and not isinstance(
                        result, asyncio.CancelledError,
                    ):
                        raise result
                    return
                result = await delivery_task
                if isinstance(result, BaseException):
                    raise result
            except Exception:
                log.exception("WATCH alert loop failed to forward event")
            finally:
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
                if not delivery_task.done():
                    delivery_task.cancel()
                    await asyncio.gather(delivery_task, return_exceptions=True)
    finally:
        bus.unsubscribe(q)


async def _wait_for_critical_drain(bus: EventBus) -> bool:
    """Wait for a stable acknowledged critical-delivery fixed point."""
    while True:
        epoch0, _, fatal = bus.critical_state()
        if fatal:
            return False
        await bus.wait_critical_idle_or_failed()
        _, _, fatal = bus.critical_state()
        if fatal:
            return False
        await asyncio.sleep(0)
        if bus.critical_state() == (epoch0, 0, False):
            return True


async def run(config_path: Path, stop: asyncio.Event | None = None) -> None:
    cfg = load_config(config_path)
    validate_runtime_config(cfg)
    validate_watch_only_release(cfg)
    setup_logging(cfg.section("log")["level"])
    data_dir = Path(cfg.section("storage")["data_dir"])
    data_dir.mkdir(parents=True, exist_ok=True)

    rpc_url = None
    providers = None
    if "providers" in cfg.raw:
        providers = cfg.section("providers")
        rpc_url = cfg.secret(providers["helius"]["rpc_url_env"])
    strategy_runtime_enabled = (
        providers is not None and "strategy" in cfg.raw and bool(rpc_url)
    )

    conn = open_db(data_dir / "memebot.db")
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            runtime_causal_floor = allocate_p3_causal_wall(
                conn, raw_wall=time.time(),
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        reconcile_unmatched_p3_buys(conn, raw_wall=time.time())
        assert_p3_buy_terminal_coverage(conn)
        if not strategy_runtime_enabled:
            assert_no_open_p3_positions(conn)
        runtime_boot_id = secrets.randbelow(2**63 - 1) + 1
    except BaseException:
        conn.close()
        raise
    boot_id = record_boot(conn, cfg.resolved_hash)
    jcfg = cfg.section("journal")
    journal = Journal(data_dir / "journal", max_bytes=jcfg["max_bytes"],
                       retention_days=jcfg["retention_days"])
    removed = journal.prune()
    log.info("journal pruned at boot", extra={"extra_fields": {"files_removed": len(removed)}})
    bus = EventBus(journal=journal)

    if strategy_runtime_enabled:
        from memebot.canonical import CanonicalResolver
        from memebot.features import FeatureEngine

        counterfactual_cfg = cfg.section("counterfactual")
        counterfactual_horizons = tuple(counterfactual_cfg["horizons_s"])
        feature_engine = FeatureEngine(
            bus,
            max_feature_mints=cfg.section("canonical")["max_feature_mints"],
        )
        canonical_resolver = None
        if cfg.section("canonical").get("enabled") is True:
            canonical_resolver = CanonicalResolver(
                conn,
                feature_engine=feature_engine,
                canonical_cfg=cfg.section("canonical"),
                safety_cfg=cfg.section("safety"),
                pumpfun_cfg=cfg.section("pumpfun"),
                config_hash=cfg.resolved_hash,
                counterfactual_horizons=counterfactual_horizons,
                runtime_boot_id=runtime_boot_id,
                runtime_causal_floor=runtime_causal_floor,
            )

    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # non-Linux dev environments
            pass

    heartbeat = Heartbeat(data_dir / "heartbeat",
                           interval=cfg.section("ops")["heartbeat_interval_s"])
    critical_stop = asyncio.Event()
    critical_tasks: list[asyncio.Task] = []
    noncritical_tasks: list[asyncio.Task] = []
    critical_task_factories = []
    noncritical_task_factories = []
    poller_client: httpx.AsyncClient | None = None
    ext_client: httpx.AsyncClient | None = None
    jup_client: httpx.AsyncClient | None = None
    if "providers" in cfg.raw:
        tracker = LifecycleTracker(
            bus,
            conn,
            cfg=cfg.section("lifecycle"),
            runtime_boot_id=runtime_boot_id,
            runtime_causal_floor=runtime_causal_floor,
        )
        critical_task_factories.append(
            lambda tracker=tracker: tracker.run(critical_stop)
        )
        pp = providers["pumpportal"]
        stream = PumpPortalStream(bus, uri=pp["ws_url"],
                                  stale_after_s=pp["stale_after_s"])
        noncritical_task_factories.append(
            lambda stream=stream: supervise("pumpportal", stream.run, bus, stop)
        )
        if rpc_url:
            poller_client = httpx.AsyncClient()
            poller = CurvePoller(bus, conn, cfg=cfg.section("curvepoller"),
                                 pumpfun=cfg.section("pumpfun"),
                                 rpc_url=rpc_url, client=poller_client,
                                 source_boot_id=runtime_boot_id)
            noncritical_task_factories.append(
                lambda poller=poller: supervise("curvepoller", poller.run, bus, stop)
            )

            # SafetyGate + GateRunner: only wired when the RPC actually works (the
            # gate's cheap on-chain tier needs it) AND [safety]/[governor.*] are
            # configured -- older/minimal configs (pre-M3) may set up the RPC for
            # the curvepoller alone with no safety section at all, which must keep
            # booting exactly as before (gate simply doesn't run).
            if "safety" in cfg.raw and "governor" in cfg.raw:
                safety_cfg = cfg.section("safety")
                governors = {name: Governor(per_minute=gcfg["per_minute"])
                            for name, gcfg in cfg.section("governor").items()}
                ext_client = httpx.AsyncClient()
                jup_client = httpx.AsyncClient()
                probes = LiveProbes(rpc_url=rpc_url, rpc_client=poller_client,
                                    ext_client=ext_client, jup_client=jup_client,
                                    conn=conn, cfg=safety_cfg, governors=governors)
                gate = SafetyGate(conn, probes=probes)
                gate_runner = GateRunner(bus, conn, gate)
                critical_task_factories.append(
                    lambda gate_runner=gate_runner: gate_runner.run(critical_stop)
                )
            else:
                log.warning("safety gate disabled: [safety]/[governor.*] not configured")
        else:
            log.warning("curvepoller disabled: rpc url env not set")
            log.warning("safety gate disabled: rpc url env not set")

    strategy = None
    if strategy_runtime_enabled:
        from memebot.broker import PaperBroker
        from memebot.counterfactual import ForwardReturnTracker
        from memebot.scoring import ConfluenceScorer
        from memebot.strategy import ClimbingStrategy

        scorer = ConfluenceScorer(cfg.section("scorer")["climbing"])
        broker = PaperBroker(cfg.section("fill"), cfg.section("pumpfun"))
        strategy = ClimbingStrategy(
            bus, conn, feature_engine=feature_engine, scorer=scorer, broker=broker,
            canonical_resolver=canonical_resolver,
            strat_cfg=cfg.section("strategy")["climbing"], pumpfun_cfg=cfg.section("pumpfun"),
            config_hash=cfg.resolved_hash, fill_cfg=cfg.section("fill"),
            exits_cfg=cfg.section("exits")["climbing"],
            stale_price_after_s=cfg.section("counterfactual")["stale_price_after_s"],
            smart_money_cfg=cfg.raw.get("smart_money", {}),
            pending_score_capacity=cfg.section("curvepoller")["max_tracked"],
            fill_event_max_age_s=cfg.section("canonical").get(
                "fill_event_max_age_s", 30.0,
            ),
            max_open_p3_positions=cfg.section("canonical").get(
                "max_open_p3_positions", 100,
            ))
        restored_p3 = strategy.reconcile(
            runtime_causal_floor=runtime_causal_floor,
            max_open_positions=cfg.section("canonical").get(
                "max_open_p3_positions", 100,
            ),
        )
        for decision_id in restored_p3:
            latest = strategy._restored_p3_latest_reports[decision_id]
            position = next(
                item for item in strategy.positions.values()
                if item.decision_id == decision_id
            )
            if (
                latest.hard_fails
                and position.entry_latest_target_report_id is not None
                and latest.safety_report_id
                > position.entry_latest_target_report_id
            ):
                strategy.zero_close_restored_p3_position(
                    decision_id=decision_id,
                    latest_report_id=latest.safety_report_id,
                    raw_wall=time.time(),
                )
        strategy.recover_pending_scores()  # restore unscored safety passes without new DDL
        tracker = ForwardReturnTracker(
            bus,
            conn,
            journal=journal,
            horizons=counterfactual_horizons,
            token_decimals=cfg.section("pumpfun")["token_decimals"],
            stale_price_after_s=counterfactual_cfg["stale_price_after_s"],
            reconcile_interval_s=cfg.section("canonical")["reconcile_interval_s"],
            price_history_retention_s=(
                counterfactual_cfg["price_history_retention_s"]
            ),
            price_history_max_samples_per_mint=(
                counterfactual_cfg["price_history_max_samples_per_mint"]
            ),
            price_history_max_mints=counterfactual_cfg["price_history_max_mints"],
            max_in_memory_pending_observations=(
                counterfactual_cfg["max_in_memory_pending_observations"]
            ),
        )
        resumed_pending = tracker.resume_from_ledger(conn)
        if resumed_pending > 0:
            earliest_pending_t0 = min(candidate.t0 for candidate in tracker._candidates)
            tracker.replay_journal(
                since_wall=earliest_pending_t0,
                until_wall=time.time(),
            )
        noncritical_task_factories.append(lambda: feature_engine.run(stop))
        critical_task_factories.append(lambda: strategy.run(critical_stop))
        noncritical_task_factories.append(lambda: tracker.run(stop))
    else:
        assert_no_open_p3_positions(conn)

    hb_task = asyncio.create_task(heartbeat.run(stop))
    prune_task = asyncio.create_task(_periodic_prune(
        journal, jcfg.get("prune_interval_s", 3600), stop))
    noncritical_tasks.extend((hb_task, prune_task))
    noncritical_tasks.extend(
        asyncio.create_task(factory()) for factory in noncritical_task_factories
    )
    critical_tasks.extend(
        asyncio.create_task(factory()) for factory in critical_task_factories
    )

    tg_cfg = cfg.raw.get("telegram", {})
    bot_token = cfg.secret(tg_cfg["bot_token_env"]) if tg_cfg.get("enabled") else None
    chat_id = cfg.secret(tg_cfg["chat_id_env"]) if tg_cfg.get("enabled") else None
    tg_client: httpx.AsyncClient | None = None
    if tg_cfg.get("enabled") and bot_token and chat_id:
        tg_client = httpx.AsyncClient()
        transport = HttpTransport(token=bot_token, client=tg_client)
        ops = TelegramOps(transport, chat_id=chat_id,
                          max_alerts_per_hour=tg_cfg["max_alerts_per_hour"])
    else:
        if tg_cfg.get("enabled"):
            log.warning("telegram enabled but bot_token/chat_id env not set; using NullOps")
        ops = NullOps()
    noncritical_tasks.append(asyncio.create_task(
        _telegram_ops_loop(bus, ops, stop, alert_on_rug=bool(tg_cfg.get("alert_on_rug", False)))))
    noncritical_tasks.append(asyncio.create_task(_trade_alert_loop(bus, ops, stop)))
    if tg_cfg.get("watch_enabled") is True:
        # Subscribe synchronously before READY: a CandidateScored published immediately by
        # another task is queued even if the WATCH coroutine has not received its first turn.
        watch_queue = bus.subscribe(CandidateScored)
        noncritical_tasks.append(asyncio.create_task(
            _watch_alert_loop(bus, ops, stop, queue=watch_queue)
        ))
    else:
        log.info("telegram WATCH feed paused by config")

    await bus.publish(AdapterHealth(t_wall=time.time(), t_mono=time.monotonic(),
                                     adapter="main", status="started", detail="idle skeleton"))
    sd_notify("READY=1")
    log.info("memebot up (idle)", extra={"extra_fields": {"config_hash": cfg.resolved_hash}})
    if strategy is not None and strategy.recovery_pending:
        noncritical_tasks.append(asyncio.create_task(
            strategy.continue_pending_score_recovery(stop)
        ))

    # Forensics asymmetry: `clean_shutdown` must record ONLY an orderly stop
    # (stop.wait() returning normally). If run() itself blows up before that —
    # e.g. mid-idle — the row must stay 0 as crash evidence. A bug in the
    # heartbeat task's beat() path (anything other than the OSError already
    # guarded inside Heartbeat.beat) must not corrupt that signal either: we
    # always reap hb_task and always finalize the journal/db, but we only
    # mark clean when the stop path itself was orderly.
    orderly = False
    try:
        await stop.wait()
        orderly = True
    finally:
        stop.set()  # ensures all tasks exit even when we got here via cancellation, not stop
        for t in noncritical_tasks:
            try:
                await t
            except Exception:
                log.exception("adapter task died; continuing shutdown")
        critical_drained = await _wait_for_critical_drain(bus)
        if not critical_drained:
            log.error("critical delivery drain failed; clean shutdown forbidden")
        critical_stop.set()
        for t in critical_tasks:
            try:
                await t
            except Exception:
                log.exception("critical task died; continuing shutdown")
        # Every task that could still be using poller_client (the curvepoller's
        # supervised task) has been reaped above, so closing here can't race an
        # in-flight request. A close error must not block shutdown forensics —
        # it's swallowed the same way adapter task failures are, above.
        if poller_client is not None:
            try:
                await poller_client.aclose()
            except Exception:
                log.exception("poller_client aclose failed; continuing shutdown")
        # Same reasoning as poller_client: the gate_runner task (which owns
        # ext_client/jup_client via LiveProbes) and the telegram ops loop task
        # (which owns tg_client via HttpTransport) are both already reaped above.
        for client, name in ((ext_client, "ext_client"), (jup_client, "jup_client"),
                             (tg_client, "tg_client")):
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    log.exception("client aclose failed; continuing shutdown",
                                  extra={"extra_fields": {"client": name}})
        journal.close()
        if orderly and critical_drained:
            mark_clean_shutdown(conn, boot_id)
        conn.close()
        log.info("memebot clean shutdown")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    args = parser.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
