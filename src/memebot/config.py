"""Config: config.toml (no secrets) + env secrets + resolved-config hash (spec §8).

Config values must be JSON-serializable scalars/tables (no TOML datetimes) — the
resolved-config hash canonicalizes via json.dumps and fails loud otherwise.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from numbers import Number
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    pass


def _exact_weight_bps(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Number):
        raise ConfigError(f"{name} weight must be a finite nonnegative exact basis point")
    try:
        weight = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ConfigError(
            f"{name} weight must be a finite nonnegative exact basis point"
        ) from None
    if not weight.is_finite() or weight < 0:
        raise ConfigError(f"{name} weight must be a finite nonnegative exact basis point")
    bps = weight * Decimal(10_000)
    if bps != bps.to_integral_value():
        raise ConfigError(f"{name} weight must be a finite nonnegative exact basis point")
    return int(bps)


def _require_positive_finite(value: object, name: str) -> None:
    if type(value) not in (int, float):
        raise ConfigError(f"{name} must be finite and greater than zero")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite or value <= 0:
        raise ConfigError(f"{name} must be finite and greater than zero")


def _require_finite_nonnegative(value: object, name: str) -> None:
    if (
        type(value) not in (int, float)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ConfigError(f"{name} must be finite and nonnegative")


@dataclass(frozen=True)
class Config:
    raw: dict[str, Any]
    resolved_hash: str  # sha256 of canonical config.toml content; secrets excluded by design
    _env: Mapping[str, str] = field(default_factory=lambda: os.environ, repr=False)

    def section(self, name: str) -> dict[str, Any]:
        try:
            return self.raw[name]
        except KeyError:
            raise ConfigError(f"missing config section {name!r}") from None

    def secret(self, env_var: str, required: bool = False) -> str | None:
        value = self._env.get(env_var)
        if required and not value:
            raise ConfigError(f"required secret env var {env_var} is not set")
        return value


def load_config(path: Path, env: Mapping[str, str] | None = None) -> Config:
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    telegram = raw.get("telegram")
    if isinstance(telegram, Mapping) and "enabled" in telegram and not isinstance(
        telegram["enabled"], bool
    ):
        raise ConfigError("telegram.enabled must be a boolean")
    if (
        isinstance(telegram, Mapping)
        and telegram.get("enabled") is True
        and not isinstance(telegram.get("watch_enabled"), bool)
    ):
        raise ConfigError("enabled telegram requires watch_enabled to be a boolean")
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    return Config(raw=raw, resolved_hash=digest, _env=env if env is not None else os.environ)


def validate_runtime_config(cfg: Config) -> None:
    canonical = cfg.raw.get("canonical")
    if not isinstance(canonical, Mapping) or type(canonical.get("enabled")) is not bool:
        raise ConfigError("canonical.enabled must be a boolean")
    live_states = canonical.get("live_states")
    if type(live_states) is not list or live_states != ["FRESH", "CLIMBING"]:
        raise ConfigError(
            "canonical.live_states must be exactly ['FRESH', 'CLIMBING']"
        )
    for name in ("max_cluster_candidates", "max_creator_history_mints"):
        value = canonical.get(name)
        if type(value) is not int or value < 1 or value > 500:
            raise ConfigError(
                f"canonical.{name} must be an integer from 1 to 500"
            )
    for name in ("resolver_version", "weights_version"):
        value = canonical.get(name)
        if type(value) is not str or len(value) == 0:
            raise ConfigError(f"canonical.{name} must be a non-empty string")
    max_feature_mints = canonical.get("max_feature_mints")
    if (
        type(max_feature_mints) is not int
        or max_feature_mints < 1
        or max_feature_mints > 10_000
    ):
        raise ConfigError(
            "canonical.max_feature_mints must be an integer from 1 to 10000"
        )
    max_open_p3_positions = canonical.get("max_open_p3_positions")
    if (
        type(max_open_p3_positions) is not int
        or max_open_p3_positions < 1
        or max_open_p3_positions > 1000
    ):
        raise ConfigError(
            "canonical.max_open_p3_positions must be an integer from 1 to 1000"
        )
    for name in (
        "liquidity_max_age_s",
        "holder_max_age_s",
        "comparison_price_max_age_s",
        "fill_event_max_age_s",
        "reconcile_interval_s",
    ):
        _require_positive_finite(canonical.get(name), f"canonical.{name}")
    strategy = cfg.raw.get("strategy")
    climbing = strategy.get("climbing") if isinstance(strategy, Mapping) else None
    if (
        isinstance(climbing, Mapping)
        and "entries_enabled" in climbing
        and type(climbing["entries_enabled"]) is not bool
    ):
        raise ConfigError("strategy.climbing.entries_enabled must be a boolean")
    if (
        isinstance(climbing, Mapping)
        and climbing.get("entries_enabled") is True
        and canonical["enabled"] is not True
    ):
        raise ConfigError("climbing entries require canonical.enabled to be true")
    canonical_weight_names = (
        "w_first_mover",
        "w_liquidity",
        "w_holder",
        "w_creator",
        "w_social",
    )
    canonical_weight_bps = []
    for name in canonical_weight_names:
        if name not in canonical:
            raise ConfigError(
                "canonical weights must sum to exactly 10,000 basis points"
            )
        canonical_weight_bps.append(
            _exact_weight_bps(canonical[name], f"canonical.{name}")
        )
    if sum(canonical_weight_bps) != 10_000:
        raise ConfigError("canonical weights must sum to exactly 10,000 basis points")
    social_weight_names = ("uri", "website", "twitter", "telegram")
    social_weights = canonical.get("social_weights")
    if (
        not isinstance(social_weights, Mapping)
        or set(social_weights) != set(social_weight_names)
    ):
        raise ConfigError(
            "canonical social weights must sum to exactly 10,000 basis points"
        )
    social_weight_bps = [
        _exact_weight_bps(
            social_weights[name],
            f"canonical.social_weights.{name}",
        )
        for name in social_weight_names
    ]
    if sum(social_weight_bps) != 10_000:
        raise ConfigError(
            "canonical social weights must sum to exactly 10,000 basis points"
        )
    scorer = cfg.raw.get("scorer")
    scorer_climbing = scorer.get("climbing") if isinstance(scorer, Mapping) else None
    for name in (
        "velocity_full_scale_sol_per_s",
        "progress_full_scale_pct",
        "age_full_scale_s",
        "smart_money_quality_full_scale_sol",
    ):
        value = (
            scorer_climbing.get(name)
            if isinstance(scorer_climbing, Mapping)
            else None
        )
        _require_positive_finite(value, f"scorer.climbing.{name}")
    scorer_weights = []
    for name in (
        "w_velocity",
        "w_progress",
        "w_age",
        "w_risk",
        "w_smart_money",
    ):
        value = (
            scorer_climbing.get(name)
            if isinstance(scorer_climbing, Mapping)
            else None
        )
        _require_finite_nonnegative(value, f"scorer.climbing.{name}")
        scorer_weights.append(value)
    try:
        scorer_weight_sum = math.fsum(scorer_weights)
    except OverflowError:
        raise ConfigError(
            "ordinary scorer weights must have a finite sum greater than zero"
        ) from None
    if not math.isfinite(scorer_weight_sum) or scorer_weight_sum <= 0:
        raise ConfigError(
            "ordinary scorer weights must have a finite sum greater than zero"
        )
    fill = cfg.raw.get("fill")
    fill_latency_min_s = (
        fill.get("latency_min_s") if isinstance(fill, Mapping) else None
    )
    _require_positive_finite(fill_latency_min_s, "fill.latency_min_s")
    safety = cfg.raw.get("safety")
    top10_holder_max_pct = (
        safety.get("top10_holder_max_pct") if isinstance(safety, Mapping) else None
    )
    try:
        holder_share_is_finite = (
            type(top10_holder_max_pct) in (int, float)
            and math.isfinite(top10_holder_max_pct)
        )
    except OverflowError:
        holder_share_is_finite = False
    if (
        not holder_share_is_finite
        or top10_holder_max_pct <= 0
        or top10_holder_max_pct > 100
    ):
        raise ConfigError(
            "safety.top10_holder_max_pct must be finite, greater than zero, "
            "and at most 100"
        )
    early_buyers = (
        safety.get("early_buyers") if isinstance(safety, Mapping) else None
    )
    signature_limit = (
        early_buyers.get("signature_limit")
        if isinstance(early_buyers, Mapping)
        else None
    )
    if (
        type(signature_limit) is not int
        or signature_limit < 1
        or signature_limit > 1000
    ):
        raise ConfigError(
            "safety.early_buyers.signature_limit must be an integer from 1 to 1000"
        )
    buyer_limit = (
        early_buyers.get("buyer_limit")
        if isinstance(early_buyers, Mapping)
        else None
    )
    if type(buyer_limit) is not int or buyer_limit < 1 or buyer_limit > 1000:
        raise ConfigError(
            "safety.early_buyers.buyer_limit must be an integer from 1 to 1000"
        )
    if buyer_limit > signature_limit:
        raise ConfigError(
            "safety.early_buyers.buyer_limit cannot exceed "
            "safety.early_buyers.signature_limit"
        )
    position_size_sol = (
        climbing.get("position_size_sol") if isinstance(climbing, Mapping) else None
    )
    _require_positive_finite(
        position_size_sol,
        "strategy.climbing.position_size_sol",
    )
    pumpfun = cfg.raw.get("pumpfun")
    graduation_sol = (
        pumpfun.get("graduation_sol") if isinstance(pumpfun, Mapping) else None
    )
    _require_positive_finite(graduation_sol, "pumpfun.graduation_sol")
    token_decimals = (
        pumpfun.get("token_decimals") if isinstance(pumpfun, Mapping) else None
    )
    if type(token_decimals) is not int or token_decimals < 0:
        raise ConfigError("pumpfun.token_decimals must be a nonnegative integer")
    exits = cfg.raw.get("exits")
    exits_climbing = exits.get("climbing") if isinstance(exits, Mapping) else None
    ladder_multiples = (
        exits_climbing.get("ladder_multiples")
        if isinstance(exits_climbing, Mapping)
        else None
    )
    ladder_fractions = (
        exits_climbing.get("ladder_fractions")
        if isinstance(exits_climbing, Mapping)
        else None
    )
    if (
        type(ladder_multiples) is not list
        or type(ladder_fractions) is not list
        or len(ladder_multiples) == 0
        or len(ladder_multiples) != len(ladder_fractions)
    ):
        raise ConfigError(
            "exits.climbing ladders must be nonempty lists of equal length"
        )
    if len(ladder_multiples) > 62:
        raise ConfigError("exits.climbing ladders must contain at most 62 entries")
    for multiple in ladder_multiples:
        try:
            multiple_is_finite = (
                type(multiple) in (int, float) and math.isfinite(multiple)
            )
        except OverflowError:
            multiple_is_finite = False
        if not multiple_is_finite or multiple <= 1:
            raise ConfigError(
                "exits.climbing.ladder_multiples values must be finite numbers "
                "greater than one"
            )
    for fraction in ladder_fractions:
        try:
            fraction_is_finite = (
                type(fraction) in (int, float) and math.isfinite(fraction)
            )
        except OverflowError:
            fraction_is_finite = False
        if not fraction_is_finite or fraction <= 0 or fraction >= 1:
            raise ConfigError(
                "exits.climbing.ladder_fractions values must be finite numbers "
                "strictly between zero and one"
            )
    try:
        ladder_fraction_sum = math.fsum(ladder_fractions)
    except OverflowError:
        ladder_fraction_sum = math.inf
    if not math.isfinite(ladder_fraction_sum) or ladder_fraction_sum >= 1:
        raise ConfigError(
            "exits.climbing.ladder_fractions sum must be finite and less than one"
        )
    counterfactual = cfg.raw.get("counterfactual")
    horizons_s = (
        counterfactual.get("horizons_s")
        if isinstance(counterfactual, Mapping)
        else None
    )
    horizons_valid = type(horizons_s) is list and 1 <= len(horizons_s) <= 32
    if horizons_valid:
        for index, horizon in enumerate(horizons_s):
            try:
                finite = (
                    type(horizon) in (int, float)
                    and math.isfinite(horizon)
                )
            except OverflowError:
                finite = False
            if (
                not finite
                or horizon <= 0
                or (index > 0 and horizon <= horizons_s[index - 1])
            ):
                horizons_valid = False
                break
    if not horizons_valid:
        raise ConfigError(
            "counterfactual.horizons_s must be a list of 1 to 32 finite numbers "
            "greater than zero in strictly increasing order"
        )
    stale_price_after_s = (
        counterfactual.get("stale_price_after_s")
        if isinstance(counterfactual, Mapping)
        else None
    )
    _require_positive_finite(
        stale_price_after_s,
        "counterfactual.stale_price_after_s",
    )
    price_history_retention_s = (
        counterfactual.get("price_history_retention_s")
        if isinstance(counterfactual, Mapping)
        else None
    )
    _require_positive_finite(
        price_history_retention_s,
        "counterfactual.price_history_retention_s",
    )
    required_retention_s = max(horizons_s) + stale_price_after_s
    try:
        retention_covers_horizons = (
            math.isfinite(required_retention_s)
            and price_history_retention_s >= required_retention_s
        )
    except OverflowError:
        retention_covers_horizons = False
    if not retention_covers_horizons:
        raise ConfigError(
            "counterfactual.price_history_retention_s must cover the maximum "
            "configured horizon plus counterfactual.stale_price_after_s"
        )
    price_history_max_samples_per_mint = (
        counterfactual.get("price_history_max_samples_per_mint")
        if isinstance(counterfactual, Mapping)
        else None
    )
    if (
        type(price_history_max_samples_per_mint) is not int
        or price_history_max_samples_per_mint <= 0
    ):
        raise ConfigError(
            "counterfactual.price_history_max_samples_per_mint must be a "
            "positive integer"
        )
    price_history_max_mints = (
        counterfactual.get("price_history_max_mints")
        if isinstance(counterfactual, Mapping)
        else None
    )
    if type(price_history_max_mints) is not int or price_history_max_mints <= 0:
        raise ConfigError(
            "counterfactual.price_history_max_mints must be a positive integer"
        )
    max_in_memory_pending_observations = (
        counterfactual.get("max_in_memory_pending_observations")
        if isinstance(counterfactual, Mapping)
        else None
    )
    if (
        type(max_in_memory_pending_observations) is not int
        or max_in_memory_pending_observations <= 0
    ):
        raise ConfigError(
            "counterfactual.max_in_memory_pending_observations must be a "
            "positive integer"
        )


def validate_watch_only_release(cfg: Config) -> None:
    strategy = cfg.raw.get("strategy")
    climbing = strategy.get("climbing") if isinstance(strategy, Mapping) else None
    if not isinstance(climbing, Mapping) or climbing.get("entries_enabled") is not False:
        raise ConfigError(
            "WATCH-only release requires [strategy.climbing].entries_enabled to be false"
        )
