import math

import pytest
from decimal import Decimal
from pathlib import Path

import memebot.config as config_module
from memebot.config import Config, ConfigError, load_config  # noqa: F401

MINIMAL = """
[storage]
data_dir = "data"
[log]
level = "INFO"
[ops]
heartbeat_interval_s = 10
[journal]
max_bytes = 1000
retention_days = 30
disk_cap_bytes = 100000
disk_alarm_fraction = 0.8
"""


def write_cfg(tmp_path, text=MINIMAL):
    p = tmp_path / "config.toml"
    p.write_text(text, encoding="utf-8")
    return p


def test_load_and_section(tmp_path):
    cfg = load_config(write_cfg(tmp_path))
    assert cfg.section("storage")["data_dir"] == "data"


def test_missing_section_raises(tmp_path):
    cfg = load_config(write_cfg(tmp_path))
    with pytest.raises(ConfigError):
        cfg.section("nope")


def test_hash_is_stable_and_content_sensitive(tmp_path):
    a = load_config(write_cfg(tmp_path))
    b = load_config(write_cfg(tmp_path))
    assert a.resolved_hash == b.resolved_hash
    c = load_config(write_cfg(tmp_path, MINIMAL.replace("10", "11")))
    assert c.resolved_hash != a.resolved_hash


def test_secret_reads_env_not_config(tmp_path):
    cfg = load_config(write_cfg(tmp_path), env={"MEMEBOT_FOO": "s3cret"})
    assert cfg.secret("MEMEBOT_FOO") == "s3cret"
    assert cfg.secret("MEMEBOT_MISSING") is None
    with pytest.raises(ConfigError):
        cfg.secret("MEMEBOT_MISSING", required=True)
    assert "s3cret" not in cfg.resolved_hash  # secrets never in the hash input


def test_hash_independent_of_env_secrets(tmp_path):
    p = write_cfg(tmp_path)
    a = load_config(p, env={"MEMEBOT_FOO": "s3cret"})
    b = load_config(p, env={"MEMEBOT_FOO": "different"})
    assert a.resolved_hash == b.resolved_hash


def test_checked_in_release_is_strict_watch_only(tmp_path):
    checked_in = load_config(Path("config.toml"), env={"MEMEBOT_SECRET": "environment-secret"})
    assert checked_in.section("strategy")["climbing"]["entries_enabled"] is False
    config_module.validate_watch_only_release(checked_in)

    config_text = Path("config.toml").read_text(encoding="utf-8")
    assert config_text.count("entries_enabled = false") == 1
    entries_enabled = load_config(
        write_cfg(
            tmp_path,
            config_text.replace("entries_enabled = false", "entries_enabled = true", 1),
        )
    )
    assert entries_enabled.resolved_hash != checked_in.resolved_hash

    invalid_raws = [
        {"strategy": {"climbing": {"entries_enabled": True}}},
        {"strategy": {"climbing": {"entries_enabled": 0}}},
        {"strategy": {"climbing": {"entries_enabled": 1}}},
        {"strategy": {"climbing": {"entries_enabled": "false"}}},
        {"strategy": {"climbing": {"entries_enabled": "release-switch-secret"}}},
        {"strategy": {"climbing": {"entries_enabled": None}}},
        {"strategy": {"climbing": {}}},
        {"strategy": {}},
        {},
        {"strategy": None},
        {"strategy": {"climbing": None}},
    ]
    errors = set()
    for raw in invalid_raws:
        cfg = Config(
            raw=raw,
            resolved_hash="not-relevant",
            _env={"MEMEBOT_SECRET": "environment-secret"},
        )
        with pytest.raises(ConfigError) as exc_info:
            config_module.validate_watch_only_release(cfg)
        message = str(exc_info.value)
        assert "release-switch-secret" not in message
        assert "environment-secret" not in message
        errors.add(message)
    assert len(errors) == 1


def test_checked_in_telegram_watch_feed_is_paused():
    cfg = load_config(Path("config.toml"))
    assert cfg.section("telegram")["watch_enabled"] is False

    config_text = Path("config.toml").read_text(encoding="utf-8")
    assert config_text.count("watch_enabled = false") == 1


@pytest.mark.parametrize(
    "watch_setting",
    (
        "",
        "watch_enabled = 0\n",
        "watch_enabled = 1\n",
        'watch_enabled = "false"\n',
        'watch_enabled = "true"\n',
    ),
)
def test_load_config_rejects_missing_or_non_bool_watch_enabled_when_telegram_enabled(
    tmp_path, watch_setting,
):
    text = MINIMAL + f"""
[telegram]
enabled = true
{watch_setting}
"""
    with pytest.raises(ConfigError, match="telegram.*watch_enabled.*boolean"):
        load_config(write_cfg(tmp_path, text))


@pytest.mark.parametrize("enabled", ("0", "1", '"false"', '"true"'))
def test_load_config_rejects_non_bool_telegram_enabled(tmp_path, enabled):
    text = MINIMAL + f"""
[telegram]
enabled = {enabled}
watch_enabled = false
"""
    with pytest.raises(ConfigError, match="telegram.enabled.*boolean"):
        load_config(write_cfg(tmp_path, text))


def test_checked_in_config_enables_bounded_early_buyer_gate():
    cfg = load_config(Path("config.toml"))
    early = cfg.section("safety")["early_buyers"]
    assert early == {
        "enabled": True,
        "signature_limit": 25,
        "buyer_limit": 20,
        "max_supply_pct": 25.0,
    }


def test_checked_in_config_has_smart_money_feature_weights():
    cfg = load_config(Path("config.toml"))
    assert cfg.section("smart_money") == {
        "min_events": 3,
        "min_realized_pnl_sol": 1.0,
        "quality_full_scale_sol": 5.0,
    }
    scorer = cfg.section("scorer")["climbing"]
    assert scorer["w_smart_money"] == 0.15
    assert scorer["smart_money_quality_full_scale_sol"] == 5.0


def test_checked_in_canonical_config_sections_are_exact():
    cfg = load_config(Path("config.toml"))

    def assert_exact(actual, expected):
        assert type(actual) is type(expected)
        if isinstance(expected, dict):
            assert actual.keys() == expected.keys()
            for key, value in expected.items():
                assert_exact(actual[key], value)
        elif isinstance(expected, list):
            assert len(actual) == len(expected)
            for actual_item, expected_item in zip(actual, expected, strict=True):
                assert_exact(actual_item, expected_item)
        else:
            assert actual == expected

    assert_exact(cfg.section("canonical"), {
        "enabled": True,
        "resolver_version": "canonical-v1",
        "weights_version": "canonical-weighted-v1",
        "live_states": ["FRESH", "CLIMBING"],
        "max_cluster_candidates": 50,
        "max_creator_history_mints": 100,
        "max_feature_mints": 1000,
        "max_open_p3_positions": 100,
        "liquidity_max_age_s": 30.0,
        "holder_max_age_s": 900.0,
        "comparison_price_max_age_s": 300.0,
        "fill_event_max_age_s": 30.0,
        "reconcile_interval_s": 60.0,
        "w_first_mover": 0.35,
        "w_liquidity": 0.25,
        "w_holder": 0.20,
        "w_creator": 0.15,
        "w_social": 0.05,
        "social_weights": {
            "uri": 0.25,
            "website": 0.25,
            "twitter": 0.25,
            "telegram": 0.25,
        },
    })
    assert_exact(cfg.section("counterfactual"), {
        "horizons_s": [3600.0, 21600.0, 86400.0],
        "stale_price_after_s": 300.0,
        "price_history_retention_s": 90000.0,
        "price_history_max_samples_per_mint": 10000,
        "price_history_max_mints": 1000,
        "max_in_memory_pending_observations": 50000,
    })


_DEFAULT_LIVE_STATES = object()


def _valid_runtime_canonical(*, enabled=True, live_states=_DEFAULT_LIVE_STATES):
    return {
        "enabled": enabled,
        "resolver_version": "canonical-v1",
        "weights_version": "canonical-weighted-v1",
        "live_states": (
            ["FRESH", "CLIMBING"]
            if live_states is _DEFAULT_LIVE_STATES
            else live_states
        ),
        "max_cluster_candidates": 50,
        "max_creator_history_mints": 100,
        "max_feature_mints": 1000,
        "max_open_p3_positions": 100,
        "liquidity_max_age_s": 30.0,
        "holder_max_age_s": 900.0,
        "comparison_price_max_age_s": 300.0,
        "fill_event_max_age_s": 30.0,
        "reconcile_interval_s": 60.0,
        "w_first_mover": 0.35,
        "w_liquidity": 0.25,
        "w_holder": 0.20,
        "w_creator": 0.15,
        "w_social": 0.05,
        "social_weights": {
            "uri": 0.25,
            "website": 0.25,
            "twitter": 0.25,
            "telegram": 0.25,
        },
    }


def _valid_runtime_scorer():
    return {
        "climbing": {
            "velocity_full_scale_sol_per_s": 0.05,
            "progress_full_scale_pct": 80.0,
            "age_full_scale_s": 600.0,
            "smart_money_quality_full_scale_sol": 5.0,
            "w_velocity": 0.40,
            "w_progress": 0.20,
            "w_age": 0.05,
            "w_risk": 0.20,
            "w_smart_money": 0.15,
        }
    }


def _valid_runtime_fill():
    return {"latency_min_s": 3.0}


def _valid_runtime_safety():
    return {
        "top10_holder_max_pct": 30.0,
        "early_buyers": {"signature_limit": 25, "buyer_limit": 20},
    }


def _valid_runtime_strategy():
    return {
        "climbing": {
            "entries_enabled": False,
            "position_size_sol": 0.2,
        }
    }


def _valid_runtime_pumpfun():
    return {"graduation_sol": 85.0, "token_decimals": 6}


def _valid_runtime_exits():
    return {
        "climbing": {
            "ladder_multiples": [1.5, 2.0, 3.0],
            "ladder_fractions": [0.4, 0.3, 0.2],
        }
    }


def _valid_runtime_counterfactual():
    return {
        "horizons_s": [3600.0, 21600.0, 86400.0],
        "stale_price_after_s": 300.0,
        "price_history_retention_s": 90000.0,
        "price_history_max_samples_per_mint": 10000,
        "price_history_max_mints": 1000,
        "max_in_memory_pending_observations": 50000,
    }


def test_canonical_enabled_is_strict_bool():
    validate = getattr(config_module, "validate_runtime_config", lambda _cfg: None)

    invalid_sections = (
        {},
        {"canonical": []},
        {"canonical": "invalid"},
        {"canonical": 1},
        {"canonical": True},
        {"canonical": None},
    )
    for raw in invalid_sections:
        cfg = Config(raw=raw, resolved_hash="not-relevant")
        with pytest.raises(ConfigError, match=r"canonical\.enabled.*boolean"):
            validate(cfg)

    for enabled in (0, 1, 0.0, 1.0, "false", "true", None):
        cfg = Config(
            raw={"canonical": {"enabled": enabled}},
            resolved_hash="not-relevant",
        )
        with pytest.raises(ConfigError, match=r"canonical\.enabled.*boolean"):
            validate(cfg)

    for enabled in (False, True):
        cfg = Config(
            raw={
                "canonical": _valid_runtime_canonical(enabled=enabled),
                "scorer": _valid_runtime_scorer(),
                "fill": _valid_runtime_fill(),
                "safety": _valid_runtime_safety(),
                "strategy": _valid_runtime_strategy(),
                "pumpfun": _valid_runtime_pumpfun(),
                "exits": _valid_runtime_exits(),
                "counterfactual": _valid_runtime_counterfactual(),
            },
            resolved_hash="not-relevant",
        )
        assert config_module.validate_runtime_config(cfg) is None


def test_canonical_live_states_are_exact():
    valid = Config(
        raw={
            "canonical": _valid_runtime_canonical(),
            "scorer": _valid_runtime_scorer(),
            "fill": _valid_runtime_fill(),
            "safety": _valid_runtime_safety(),
            "strategy": _valid_runtime_strategy(),
            "pumpfun": _valid_runtime_pumpfun(),
            "exits": _valid_runtime_exits(),
            "counterfactual": _valid_runtime_counterfactual(),
        },
        resolved_hash="not-relevant",
    )
    assert config_module.validate_runtime_config(valid) is None

    invalid_live_states = (
        None,
        "FRESH,CLIMBING",
        ("FRESH", "CLIMBING"),
        {"FRESH", "CLIMBING"},
        [],
        ["FRESH"],
        ["CLIMBING", "FRESH"],
        ["FRESH", "CLIMBING", "FRESH"],
        ["FRESH", "CLIMBING", "GRADUATED"],
        ["fresh", "CLIMBING"],
        ["FRESH", 1],
    )
    for live_states in invalid_live_states:
        cfg = Config(
            raw={
                "canonical": _valid_runtime_canonical(live_states=live_states)
            },
            resolved_hash="not-relevant",
        )
        with pytest.raises(
            ConfigError,
            match=r"canonical\.live_states.*exactly.*FRESH.*CLIMBING",
        ):
            config_module.validate_runtime_config(cfg)


def test_canonical_entries_require_canonical_enabled():
    def runtime_cfg(*, canonical_enabled, entries_enabled):
        return Config(
            raw={
                "canonical": _valid_runtime_canonical(enabled=canonical_enabled),
                "strategy": {
                    "climbing": {
                        "entries_enabled": entries_enabled,
                        "position_size_sol": 0.2,
                    }
                },
                "scorer": _valid_runtime_scorer(),
                "fill": _valid_runtime_fill(),
                "safety": _valid_runtime_safety(),
                "pumpfun": _valid_runtime_pumpfun(),
                "exits": _valid_runtime_exits(),
                "counterfactual": _valid_runtime_counterfactual(),
            },
            resolved_hash="not-relevant",
        )

    invalid_entries = (0, 1, 0.0, 1.0, "false", "true", None, [], {})
    for canonical_enabled in (False, True):
        for entries_enabled in invalid_entries:
            with pytest.raises(
                ConfigError,
                match=r"strategy\.climbing\.entries_enabled.*boolean",
            ):
                config_module.validate_runtime_config(
                    runtime_cfg(
                        canonical_enabled=canonical_enabled,
                        entries_enabled=entries_enabled,
                    )
                )

    with pytest.raises(
        ConfigError,
        match=r"climbing entries.*canonical\.enabled.*true",
    ):
        config_module.validate_runtime_config(
            runtime_cfg(canonical_enabled=False, entries_enabled=True)
        )

    assert (
        config_module.validate_runtime_config(
            runtime_cfg(canonical_enabled=True, entries_enabled=True)
        )
        is None
    )
    assert (
        config_module.validate_runtime_config(
            runtime_cfg(canonical_enabled=False, entries_enabled=False)
        )
        is None
    )


def test_weight_bps_values_are_exact_decimal_integers():
    canonical_keys = (
        "w_first_mover",
        "w_liquidity",
        "w_holder",
        "w_creator",
        "w_social",
    )
    social_keys = ("uri", "website", "twitter", "telegram")

    # Complementary fractional-BPS changes preserve each aggregate at 10,000 BPS,
    # so a later aggregate validator cannot mask a rounded scalar implementation.
    for mutations in (
        (
            ("canonical", "w_first_mover", 0.35001),
            ("canonical", "w_liquidity", 0.24999),
        ),
        (
            ("social_weights", "uri", 0.25001),
            ("social_weights", "website", 0.24999),
        ),
    ):
        raw = load_config(Path("config.toml")).raw
        for section, key, value in mutations:
            target = (
                raw["canonical"]
                if section == "canonical"
                else raw["canonical"][section]
            )
            target[key] = value
        with pytest.raises(ConfigError, match=r"weight.*basis point"):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    invalid_scalars = (
        True,
        False,
        "0.25",
        None,
        float("nan"),
        float("inf"),
        float("-inf"),
        -0.0001,
    )
    for section, keys in (
        ("canonical", canonical_keys),
        ("social_weights", social_keys),
    ):
        for key in keys:
            for invalid in invalid_scalars:
                raw = load_config(Path("config.toml")).raw
                target = (
                    raw["canonical"]
                    if section == "canonical"
                    else raw["canonical"][section]
                )
                target[key] = invalid
                with pytest.raises(ConfigError, match=r"weight.*basis point"):
                    config_module.validate_runtime_config(
                        Config(raw=raw, resolved_hash="not-relevant")
                    )

    for mutations in (
        (
            ("canonical", "w_first_mover", 0.0),
            ("canonical", "w_liquidity", 0.60),
        ),
        (
            ("social_weights", "uri", 0.0),
            ("social_weights", "website", 0.50),
        ),
        (
            ("canonical", "w_first_mover", 0.0001),
            ("canonical", "w_liquidity", 0.5999),
        ),
        (
            ("social_weights", "uri", 0.0001),
            ("social_weights", "website", 0.4999),
        ),
    ):
        raw = load_config(Path("config.toml")).raw
        for section, key, value in mutations:
            target = (
                raw["canonical"]
                if section == "canonical"
                else raw["canonical"][section]
            )
            target[key] = value
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )

    valid = load_config(Path("config.toml"))
    assert config_module.validate_runtime_config(valid) is None


def test_canonical_weight_bps_sum_is_exact():
    for social_weight in (0.0499, 0.0501):
        raw = load_config(Path("config.toml")).raw
        raw["canonical"]["w_social"] = social_weight
        with pytest.raises(
            ConfigError,
            match=r"canonical weights.*10,000 basis points",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["canonical"]["w_social"]
    with pytest.raises(
        ConfigError,
        match=r"canonical weights.*10,000 basis points",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    raw = load_config(Path("config.toml")).raw
    for name in (
        "w_first_mover",
        "w_liquidity",
        "w_holder",
        "w_creator",
        "w_social",
    ):
        del raw["canonical"][name]
    with pytest.raises(
        ConfigError,
        match=r"canonical weights.*10,000 basis points",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    assert (
        config_module.validate_runtime_config(load_config(Path("config.toml"))) is None
    )


def test_social_weight_bps_sum_is_exact():
    for name in ("uri", "website", "twitter", "telegram"):
        for weight in (0.2499, 0.2501):
            raw = load_config(Path("config.toml")).raw
            raw["canonical"]["social_weights"][name] = weight
            with pytest.raises(
                ConfigError,
                match=r"canonical social weights.*10,000 basis points",
            ):
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )

    raw = load_config(Path("config.toml")).raw
    del raw["canonical"]["social_weights"]["telegram"]
    with pytest.raises(
        ConfigError,
        match=r"canonical social weights.*10,000 basis points",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    raw = load_config(Path("config.toml")).raw
    del raw["canonical"]["social_weights"]
    with pytest.raises(
        ConfigError,
        match=r"canonical social weights.*10,000 basis points",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    raw = load_config(Path("config.toml")).raw
    raw["canonical"]["social_weights"]["unexpected"] = 0
    with pytest.raises(
        ConfigError,
        match=r"canonical social weights.*10,000 basis points",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    assert (
        config_module.validate_runtime_config(load_config(Path("config.toml"))) is None
    )


def test_ordinary_scorer_divisor_is_strict_positive_finite():
    divisor_names = (
        "velocity_full_scale_sol_per_s",
        "progress_full_scale_pct",
        "age_full_scale_s",
        "smart_money_quality_full_scale_sol",
    )
    invalid_values = (
        True,
        False,
        0,
        0.0,
        -1,
        -0.001,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "1.0",
        None,
    )

    for name in divisor_names:
        for invalid in invalid_values:
            raw = load_config(Path("config.toml")).raw
            raw["scorer"]["climbing"][name] = invalid
            with pytest.raises(
                ConfigError,
                match=rf"scorer\.climbing\.{name}.*finite.*greater than zero",
            ):
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )

        raw = load_config(Path("config.toml")).raw
        del raw["scorer"]["climbing"][name]
        with pytest.raises(
            ConfigError,
            match=rf"scorer\.climbing\.{name}.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

        raw = load_config(Path("config.toml")).raw
        raw["scorer"]["climbing"][name] = 1
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["scorer"]
    invalid_structures.append(raw)
    for scorer in (None, {}, {"climbing": None}):
        raw = load_config(Path("config.toml")).raw
        raw["scorer"] = scorer
        invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(
            ConfigError,
            match=r"scorer\.climbing.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    assert (
        config_module.validate_runtime_config(load_config(Path("config.toml"))) is None
    )


def test_ordinary_scorer_weights_are_finite_nonnegative():
    weight_names = (
        "w_velocity",
        "w_progress",
        "w_age",
        "w_risk",
        "w_smart_money",
    )

    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        True,
        False,
        -1,
        -0.001,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "0.1",
        None,
        [],
        IntSubclass(1),
        FloatSubclass(0.1),
    )

    for name in weight_names:
        for invalid in invalid_values:
            raw = load_config(Path("config.toml")).raw
            raw["scorer"]["climbing"][name] = invalid
            with pytest.raises(
                ConfigError,
                match=rf"scorer\.climbing\.{name}.*finite.*nonnegative",
            ):
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )

        raw = load_config(Path("config.toml")).raw
        del raw["scorer"]["climbing"][name]
        with pytest.raises(
            ConfigError,
            match=rf"scorer\.climbing\.{name}.*finite.*nonnegative",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

        for valid in (0, 0.0, 1, 0.25):
            raw = load_config(Path("config.toml")).raw
            raw["scorer"]["climbing"][name] = valid
            assert (
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )
                is None
            )

    assert (
        config_module.validate_runtime_config(load_config(Path("config.toml"))) is None
    )


def test_ordinary_scorer_weight_sum_is_positive():
    weight_names = (
        "w_velocity",
        "w_progress",
        "w_age",
        "w_risk",
        "w_smart_money",
    )

    raw = load_config(Path("config.toml")).raw
    for name in weight_names:
        raw["scorer"]["climbing"][name] = 0.0
    with pytest.raises(
        ConfigError,
        match=r"ordinary scorer weights.*finite.*greater than zero",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    raw = load_config(Path("config.toml")).raw
    for name in weight_names:
        raw["scorer"]["climbing"][name] = 1e308
    with pytest.raises(
        ConfigError,
        match=r"ordinary scorer weights.*finite.*greater than zero",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for positive_name in weight_names:
        raw = load_config(Path("config.toml")).raw
        for name in weight_names:
            raw["scorer"]["climbing"][name] = 0.0
        raw["scorer"]["climbing"][positive_name] = 1e-300
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )

    assert (
        config_module.validate_runtime_config(load_config(Path("config.toml"))) is None
    )


def test_execution_fill_latency_bound():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        True,
        False,
        0,
        0.0,
        -1,
        -0.001,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "3.0",
        None,
        [],
        10**1000,
        -(10**1000),
        IntSubclass(1),
        FloatSubclass(1.0),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["fill"]["latency_min_s"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"fill\.latency_min_s.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["fill"]
    invalid_structures.append(raw)
    for fill in (None, "invalid", [], {}, {"other": 3.0}):
        raw = load_config(Path("config.toml")).raw
        raw["fill"] = fill
        invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(
            ConfigError,
            match=r"fill\.latency_min_s.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for valid in (
        1,
        0.001,
        3.0,
        float.fromhex("0x0.0000000000001p-1022"),
        float.fromhex("0x1.fffffffffffffp+1023"),
    ):
        raw = load_config(Path("config.toml")).raw
        raw["fill"]["latency_min_s"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_holder_share_bound():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        True,
        False,
        0,
        0.0,
        -1,
        -0.001,
        100.00001,
        float.fromhex("0x1.fffffffffffffp+1023"),
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "30.0",
        None,
        [],
        10**1000,
        -(10**1000),
        IntSubclass(30),
        FloatSubclass(30.0),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["safety"]["top10_holder_max_pct"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"safety\.top10_holder_max_pct.*finite.*greater than zero.*100",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["safety"]
    invalid_structures.append(raw)
    for safety in (None, "invalid", [], {}, {"other": 30.0}):
        raw = load_config(Path("config.toml")).raw
        raw["safety"] = safety
        invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(
            ConfigError,
            match=r"safety\.top10_holder_max_pct.*finite.*greater than zero.*100",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for valid in (
        1,
        0.001,
        30.0,
        100,
        100.0,
        float.fromhex("0x0.0000000000001p-1022"),
    ):
        raw = load_config(Path("config.toml")).raw
        raw["safety"]["top10_holder_max_pct"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_order_size_bound():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        True,
        False,
        0,
        0.0,
        -1,
        -0.001,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "0.2",
        None,
        [],
        10**1000,
        -(10**1000),
        IntSubclass(1),
        FloatSubclass(1.0),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["strategy"]["climbing"]["position_size_sol"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"strategy\.climbing\.position_size_sol.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["strategy"]
    invalid_structures.append(raw)
    for strategy in (
        None,
        "invalid",
        [],
        {},
        {"climbing": None},
        {"climbing": {"entries_enabled": False}},
    ):
        raw = load_config(Path("config.toml")).raw
        raw["strategy"] = strategy
        invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(
            ConfigError,
            match=r"strategy\.climbing\.position_size_sol.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for valid in (
        1,
        0.001,
        0.2,
        float.fromhex("0x0.0000000000001p-1022"),
        float.fromhex("0x1.fffffffffffffp+1023"),
    ):
        raw = load_config(Path("config.toml")).raw
        raw["strategy"]["climbing"]["position_size_sol"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_graduation_threshold_bound():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        True,
        False,
        0,
        0.0,
        -1,
        -0.001,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "85.0",
        None,
        [],
        10**1000,
        -(10**1000),
        IntSubclass(85),
        FloatSubclass(85.0),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["pumpfun"]["graduation_sol"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"pumpfun\.graduation_sol.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["pumpfun"]
    invalid_structures.append(raw)
    for pumpfun in (None, "invalid", [], {}, {"other": 85.0}):
        raw = load_config(Path("config.toml")).raw
        raw["pumpfun"] = pumpfun
        invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(
            ConfigError,
            match=r"pumpfun\.graduation_sol.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for valid in (
        1,
        0.001,
        85.0,
        float.fromhex("0x0.0000000000001p-1022"),
        float.fromhex("0x1.fffffffffffffp+1023"),
    ):
        raw = load_config(Path("config.toml")).raw
        raw["pumpfun"]["graduation_sol"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_token_decimals_bound():
    class IntSubclass(int):
        pass

    invalid_values = (
        True,
        False,
        -1,
        -(10**1000),
        0.0,
        1.0,
        6.0,
        float("nan"),
        float("inf"),
        float("-inf"),
        0j,
        6 + 0j,
        "6",
        None,
        [],
        (),
        {},
        set(),
        Decimal("6"),
        IntSubclass(6),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["pumpfun"]["token_decimals"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"pumpfun\.token_decimals.*nonnegative.*integer",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["pumpfun"]["token_decimals"]
    with pytest.raises(
        ConfigError,
        match=r"pumpfun\.token_decimals.*nonnegative.*integer",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["pumpfun"]
    invalid_structures.append(raw)
    for pumpfun in (None, "invalid", [], 6):
        raw = load_config(Path("config.toml")).raw
        raw["pumpfun"] = pumpfun
        invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(ConfigError):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for valid in (0, 1, 6, 10**1000):
        raw = load_config(Path("config.toml")).raw
        raw["pumpfun"]["token_decimals"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_early_buyer_signature_limit_bound():
    class IntSubclass(int):
        pass

    invalid_values = (
        True,
        False,
        0,
        -1,
        1001,
        10**1000,
        1.0,
        25.0,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        "25",
        None,
        [],
        (),
        {},
        set(),
        Decimal("25"),
        IntSubclass(25),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["safety"]["early_buyers"]["signature_limit"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"safety\.early_buyers\.signature_limit.*integer.*1.*1000",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["safety"]
    invalid_structures.append(raw)
    for safety in (
        None,
        "invalid",
        [],
        {},
        {"top10_holder_max_pct": 30.0},
        {"top10_holder_max_pct": 30.0, "early_buyers": None},
        {"top10_holder_max_pct": 30.0, "early_buyers": "invalid"},
        {"top10_holder_max_pct": 30.0, "early_buyers": []},
        {"top10_holder_max_pct": 30.0, "early_buyers": {}},
    ):
        raw = load_config(Path("config.toml")).raw
        raw["safety"] = safety
        invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(ConfigError):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for valid in (1, 25, 1000):
        raw = load_config(Path("config.toml")).raw
        raw["safety"]["early_buyers"]["signature_limit"] = valid
        raw["safety"]["early_buyers"]["buyer_limit"] = min(
            raw["safety"]["early_buyers"]["buyer_limit"],
            valid,
        )
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_early_buyer_buyer_limit_bound():
    class IntSubclass(int):
        pass

    invalid_values = (
        True,
        False,
        0,
        -1,
        1001,
        10**1000,
        1.0,
        20.0,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        "20",
        None,
        [],
        (),
        {},
        set(),
        Decimal("20"),
        IntSubclass(20),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["safety"]["early_buyers"]["buyer_limit"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"safety\.early_buyers\.buyer_limit.*integer.*1.*1000",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["safety"]["early_buyers"]["buyer_limit"]
    with pytest.raises(
        ConfigError,
        match=r"safety\.early_buyers\.buyer_limit.*integer.*1.*1000",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["safety"]
    invalid_structures.append(raw)
    for safety in (
        None,
        "invalid",
        [],
        {},
        {"top10_holder_max_pct": 30.0},
        {"top10_holder_max_pct": 30.0, "early_buyers": None},
        {"top10_holder_max_pct": 30.0, "early_buyers": "invalid"},
        {"top10_holder_max_pct": 30.0, "early_buyers": []},
        {"top10_holder_max_pct": 30.0, "early_buyers": {}},
    ):
        raw = load_config(Path("config.toml")).raw
        raw["safety"] = safety
        invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(ConfigError):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for valid in (1, 20, 1000):
        raw = load_config(Path("config.toml")).raw
        raw["safety"]["early_buyers"]["buyer_limit"] = valid
        raw["safety"]["early_buyers"]["signature_limit"] = max(
            raw["safety"]["early_buyers"]["signature_limit"],
            valid,
        )
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_early_buyer_limit_coupling():
    raw = load_config(Path("config.toml")).raw
    raw["safety"]["early_buyers"]["signature_limit"] = 19
    raw["safety"]["early_buyers"]["buyer_limit"] = 20
    with pytest.raises(
        ConfigError,
        match=(
            r"safety\.early_buyers\.buyer_limit.*cannot exceed"
            r".*safety\.early_buyers\.signature_limit"
        ),
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for signature_limit, buyer_limit in ((20, 20), (21, 20), (1000, 1)):
        raw = load_config(Path("config.toml")).raw
        raw["safety"]["early_buyers"]["signature_limit"] = signature_limit
        raw["safety"]["early_buyers"]["buyer_limit"] = buyer_limit
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_counterfactual_horizon_list_is_strict_bounded_and_ordered():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    class ListSubclass(list):
        pass

    invalid_horizons = (
        [],
        [True],
        [False],
        [0],
        [0.0],
        [-1],
        [-0.001],
        [float("nan")],
        [float("inf")],
        [float("-inf")],
        [10**1000],
        [1 + 0j],
        ["1"],
        [None],
        [Decimal("1")],
        [IntSubclass(1)],
        [FloatSubclass(1.0)],
        [1, 1.0],
        [2, 1],
        list(range(1, 34)),
        (1, 2),
        ListSubclass([1, 2]),
        "1,2",
        None,
        {},
    )
    for invalid in invalid_horizons:
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["horizons_s"] = invalid
        with pytest.raises(
            ConfigError,
            match=(
                r"counterfactual\.horizons_s.*list.*1.*32.*finite"
                r".*greater than zero.*strictly increasing"
            ),
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    invalid_structures = []
    raw = load_config(Path("config.toml")).raw
    del raw["counterfactual"]
    invalid_structures.append(raw)
    for counterfactual in (
        None,
        "invalid",
        [],
        {},
        {"other": [1, 2]},
    ):
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"] = counterfactual
        invalid_structures.append(raw)

    raw = load_config(Path("config.toml")).raw
    del raw["counterfactual"]["horizons_s"]
    invalid_structures.append(raw)

    for raw in invalid_structures:
        with pytest.raises(
            ConfigError,
            match=(
                r"counterfactual\.horizons_s.*list.*1.*32.*finite"
                r".*greater than zero.*strictly increasing"
            ),
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for valid in (
        [1, 2.5, 3, 4.25],
        list(range(1, 33)),
        [
            float.fromhex("0x0.0000000000001p-1022"),
            1,
            float.fromhex("0x1.fffffffffffffp+1023"),
        ],
    ):
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["horizons_s"] = valid
        raw["counterfactual"]["stale_price_after_s"] = float.fromhex(
            "0x0.0000000000001p-1022"
        )
        raw["counterfactual"]["price_history_retention_s"] = float.fromhex(
            "0x1.fffffffffffffp+1023"
        )
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_counterfactual_retention_is_positive_finite():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        True,
        False,
        0,
        0.0,
        -1,
        -0.001,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "90000.0",
        None,
        [],
        (),
        {},
        set(),
        Decimal("90000.0"),
        10**1000,
        -(10**1000),
        IntSubclass(1),
        FloatSubclass(1.0),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["price_history_retention_s"] = invalid
        with pytest.raises(
            ConfigError,
            match=(
                r"counterfactual\.price_history_retention_s"
                r".*finite.*greater than zero"
            ),
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["counterfactual"]["price_history_retention_s"]
    with pytest.raises(
        ConfigError,
        match=(
            r"counterfactual\.price_history_retention_s"
            r".*finite.*greater than zero"
        ),
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    min_subnormal = float.fromhex("0x0.0000000000001p-1022")
    max_finite = float.fromhex("0x1.fffffffffffffp+1023")
    for valid, horizon, stale_age in (
        (1, 0.5, 0.5),
        (0.001, 0.0005, 0.0005),
        (90000.0, 86400.0, 300.0),
        (min_subnormal * 2, min_subnormal, min_subnormal),
        (max_finite, max_finite, min_subnormal),
    ):
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["horizons_s"] = [horizon]
        raw["counterfactual"]["stale_price_after_s"] = stale_age
        raw["counterfactual"]["price_history_retention_s"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_counterfactual_stale_price_age_is_positive_finite():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        True,
        False,
        0,
        0.0,
        -1,
        -0.001,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "300.0",
        None,
        [],
        (),
        {},
        set(),
        Decimal("300.0"),
        10**1000,
        -(10**1000),
        IntSubclass(1),
        FloatSubclass(1.0),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["stale_price_after_s"] = invalid
        with pytest.raises(
            ConfigError,
            match=(
                r"counterfactual\.stale_price_after_s"
                r".*finite.*greater than zero"
            ),
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["counterfactual"]["stale_price_after_s"]
    with pytest.raises(
        ConfigError,
        match=(
            r"counterfactual\.stale_price_after_s"
            r".*finite.*greater than zero"
        ),
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for valid in (
        1,
        0.001,
        300.0,
        float.fromhex("0x0.0000000000001p-1022"),
        float.fromhex("0x1.fffffffffffffp+1023"),
    ):
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["horizons_s"] = [
            float.fromhex("0x0.0000000000001p-1022")
        ]
        raw["counterfactual"]["stale_price_after_s"] = valid
        raw["counterfactual"]["price_history_retention_s"] = float.fromhex(
            "0x1.fffffffffffffp+1023"
        )
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_counterfactual_retention_covers_max_horizon():
    horizons = [1.0, 2.5, 7.0]
    stale_price_after_s = 3.0
    required_retention = max(horizons) + stale_price_after_s

    for retention in (required_retention, required_retention + 0.001):
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["horizons_s"] = horizons
        raw["counterfactual"]["stale_price_after_s"] = stale_price_after_s
        raw["counterfactual"]["price_history_retention_s"] = retention
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )

    invalid_cases = (
        (horizons, stale_price_after_s, required_retention - 0.001),
        (
            [float.fromhex("0x1.fffffffffffffp+1023")],
            float.fromhex("0x1.fffffffffffffp+1023"),
            float.fromhex("0x1.fffffffffffffp+1023"),
        ),
    )
    for case_horizons, case_stale_age, retention in invalid_cases:
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["horizons_s"] = case_horizons
        raw["counterfactual"]["stale_price_after_s"] = case_stale_age
        raw["counterfactual"]["price_history_retention_s"] = retention
        with pytest.raises(
            ConfigError,
            match=(
                r"counterfactual\.price_history_retention_s"
                r".*max.*horizon.*stale_price_after_s"
            ),
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )


def test_counterfactual_sample_cap():
    class IntSubclass(int):
        pass

    invalid_values = (
        True,
        False,
        0,
        -1,
        1.0,
        Decimal("1"),
        1 + 0j,
        "1",
        None,
        [],
        (),
        {},
        set(),
        IntSubclass(1),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["price_history_max_samples_per_mint"] = invalid
        with pytest.raises(
            ConfigError,
            match=(
                r"counterfactual\.price_history_max_samples_per_mint"
                r".*positive integer"
            ),
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["counterfactual"]["price_history_max_samples_per_mint"]
    with pytest.raises(
        ConfigError,
        match=(
            r"counterfactual\.price_history_max_samples_per_mint"
            r".*positive integer"
        ),
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for valid in (1, 10**1000):
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["price_history_max_samples_per_mint"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_counterfactual_mint_cap():
    class IntSubclass(int):
        pass

    invalid_values = (
        True,
        False,
        0,
        -1,
        1.0,
        Decimal("1"),
        1 + 0j,
        "1",
        None,
        [],
        (),
        {},
        set(),
        IntSubclass(1),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["price_history_max_mints"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"counterfactual\.price_history_max_mints.*positive integer",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["counterfactual"]["price_history_max_mints"]
    with pytest.raises(
        ConfigError,
        match=r"counterfactual\.price_history_max_mints.*positive integer",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for valid in (1, 10**1000):
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["price_history_max_mints"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_counterfactual_pending_cap():
    class IntSubclass(int):
        pass

    invalid_values = (
        True,
        False,
        0,
        -1,
        1.0,
        Decimal("1"),
        1 + 0j,
        "1",
        None,
        [],
        (),
        {},
        set(),
        IntSubclass(1),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["max_in_memory_pending_observations"] = invalid
        with pytest.raises(
            ConfigError,
            match=(
                r"counterfactual\.max_in_memory_pending_observations"
                r".*positive integer"
            ),
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["counterfactual"]["max_in_memory_pending_observations"]
    with pytest.raises(
        ConfigError,
        match=(
            r"counterfactual\.max_in_memory_pending_observations"
            r".*positive integer"
        ),
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for valid in (1, 10**1000):
        raw = load_config(Path("config.toml")).raw
        raw["counterfactual"]["max_in_memory_pending_observations"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_feature_mint_cap():
    class IntSubclass(int):
        pass

    invalid_values = (
        True,
        False,
        0,
        -1,
        10_001,
        10**1000,
        1.0,
        Decimal("1"),
        1 + 0j,
        "1",
        None,
        [],
        (),
        {},
        set(),
        IntSubclass(1),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["canonical"]["max_feature_mints"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"canonical\.max_feature_mints.*integer.*1.*10000",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["canonical"]["max_feature_mints"]
    with pytest.raises(
        ConfigError,
        match=r"canonical\.max_feature_mints.*integer.*1.*10000",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for valid in (1, 10_000):
        raw = load_config(Path("config.toml")).raw
        raw["canonical"]["max_feature_mints"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_open_p3_position_cap():
    class IntSubclass(int):
        pass

    invalid_values = (
        True,
        False,
        0,
        -1,
        1001,
        10**1000,
        1.0,
        Decimal("1"),
        1 + 0j,
        "1",
        None,
        [],
        (),
        {},
        set(),
        IntSubclass(1),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["canonical"]["max_open_p3_positions"] = invalid
        with pytest.raises(
            ConfigError,
            match=r"canonical\.max_open_p3_positions.*integer.*1.*1000",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    del raw["canonical"]["max_open_p3_positions"]
    with pytest.raises(
        ConfigError,
        match=r"canonical\.max_open_p3_positions.*integer.*1.*1000",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for valid in (1, 1000):
        raw = load_config(Path("config.toml")).raw
        raw["canonical"]["max_open_p3_positions"] = valid
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_canonical_age_and_interval_fields_are_positive_finite():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    field_names = (
        "liquidity_max_age_s",
        "holder_max_age_s",
        "comparison_price_max_age_s",
        "fill_event_max_age_s",
        "reconcile_interval_s",
    )
    invalid_values = (
        True,
        False,
        0,
        0.0,
        -1,
        -0.001,
        float("nan"),
        float("inf"),
        float("-inf"),
        1 + 0j,
        1 + 2j,
        "30.0",
        None,
        [],
        (),
        {},
        set(),
        Decimal("30.0"),
        10**1000,
        -(10**1000),
        IntSubclass(1),
        FloatSubclass(1.0),
    )
    for name in field_names:
        for invalid in invalid_values:
            raw = load_config(Path("config.toml")).raw
            raw["canonical"][name] = invalid
            with pytest.raises(
                ConfigError,
                match=rf"canonical\.{name}.*finite.*greater than zero",
            ):
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )

        raw = load_config(Path("config.toml")).raw
        del raw["canonical"][name]
        with pytest.raises(
            ConfigError,
            match=rf"canonical\.{name}.*finite.*greater than zero",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

        for valid in (
            1,
            0.001,
            float.fromhex("0x0.0000000000001p-1022"),
            float.fromhex("0x1.fffffffffffffp+1023"),
        ):
            raw = load_config(Path("config.toml")).raw
            raw["canonical"][name] = valid
            assert (
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )
                is None
            )


def test_canonical_cluster_and_creator_caps():
    class IntSubclass(int):
        pass

    field_names = (
        "max_cluster_candidates",
        "max_creator_history_mints",
    )
    invalid_values = (
        True,
        False,
        0,
        -1,
        501,
        10**1000,
        1.0,
        Decimal("1"),
        1 + 0j,
        "1",
        None,
        [],
        (),
        {},
        set(),
        IntSubclass(1),
    )
    for name in field_names:
        for invalid in invalid_values:
            raw = load_config(Path("config.toml")).raw
            raw["canonical"][name] = invalid
            with pytest.raises(
                ConfigError,
                match=rf"canonical\.{name}.*integer.*1.*500",
            ):
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )

        raw = load_config(Path("config.toml")).raw
        del raw["canonical"][name]
        with pytest.raises(
            ConfigError,
            match=rf"canonical\.{name}.*integer.*1.*500",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

        for valid in (1, 500):
            raw = load_config(Path("config.toml")).raw
            raw["canonical"][name] = valid
            assert (
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )
                is None
            )


def test_canonical_version_strings_are_nonempty():
    class StrSubclass(str):
        pass

    field_names = ("resolver_version", "weights_version")
    invalid_values = (
        "",
        True,
        False,
        0,
        1,
        0.0,
        1.0,
        None,
        [],
        (),
        {},
        set(),
        StrSubclass("version"),
    )
    for name in field_names:
        for invalid in invalid_values:
            raw = load_config(Path("config.toml")).raw
            raw["canonical"][name] = invalid
            with pytest.raises(
                ConfigError,
                match=rf"canonical\.{name}.*non-empty string",
            ):
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )

        raw = load_config(Path("config.toml")).raw
        del raw["canonical"][name]
        with pytest.raises(
            ConfigError,
            match=rf"canonical\.{name}.*non-empty string",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

        for valid in ("v", " "):
            raw = load_config(Path("config.toml")).raw
            raw["canonical"][name] = valid
            assert (
                config_module.validate_runtime_config(
                    Config(raw=raw, resolved_hash="not-relevant")
                )
                is None
            )


def test_climbing_ladder_lengths_match_and_are_nonempty():
    class ListSubclass(list):
        pass

    invalid_ladders = (
        ([], []),
        ([], [0.25]),
        ([1.5], []),
        ([1.5], [0.25, 0.25]),
        ([1.5, 2.0], [0.25]),
        ((1.5,), [0.25]),
        ([1.5], (0.25,)),
        (ListSubclass([1.5]), [0.25]),
        ([1.5], ListSubclass([0.25])),
        (None, [0.25]),
        ([1.5], None),
    )
    for multiples, fractions in invalid_ladders:
        raw = load_config(Path("config.toml")).raw
        raw["exits"]["climbing"]["ladder_multiples"] = multiples
        raw["exits"]["climbing"]["ladder_fractions"] = fractions
        with pytest.raises(
            ConfigError,
            match=r"exits\.climbing.*ladders.*nonempty.*equal length",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    for name in ("ladder_multiples", "ladder_fractions"):
        raw = load_config(Path("config.toml")).raw
        del raw["exits"]["climbing"][name]
        with pytest.raises(
            ConfigError,
            match=r"exits\.climbing.*ladders.*nonempty.*equal length",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    assert (
        config_module.validate_runtime_config(load_config(Path("config.toml"))) is None
    )


def test_climbing_ladder_count_fits_signed_mask():
    raw = load_config(Path("config.toml")).raw
    raw["exits"]["climbing"]["ladder_multiples"] = [1.5] * 62
    raw["exits"]["climbing"]["ladder_fractions"] = [0.01] * 62
    assert (
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )
        is None
    )

    raw = load_config(Path("config.toml")).raw
    raw["exits"]["climbing"]["ladder_multiples"] = [1.5] * 63
    raw["exits"]["climbing"]["ladder_fractions"] = [0.01] * 63
    with pytest.raises(
        ConfigError,
        match=r"exits\.climbing.*ladders.*at most 62",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )


def test_climbing_ladder_multiples_are_finite_above_one():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        1,
        1.0,
        0,
        0.0,
        -1,
        -1.0,
        True,
        False,
        float("nan"),
        float("inf"),
        float("-inf"),
        10**400,
        "1.5",
        None,
        [],
        {},
        IntSubclass(2),
        FloatSubclass(1.5),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["exits"]["climbing"]["ladder_multiples"] = [invalid]
        raw["exits"]["climbing"]["ladder_fractions"] = [0.25]
        with pytest.raises(
            ConfigError,
            match=r"exits\.climbing\.ladder_multiples.*finite.*greater than one",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    raw["exits"]["climbing"]["ladder_multiples"] = [1.5, float("nan")]
    raw["exits"]["climbing"]["ladder_fractions"] = [0.25, 0.25]
    with pytest.raises(
        ConfigError,
        match=r"exits\.climbing\.ladder_multiples.*finite.*greater than one",
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for valid in (2, 1.0000000000000002):
        raw = load_config(Path("config.toml")).raw
        raw["exits"]["climbing"]["ladder_multiples"] = [valid]
        raw["exits"]["climbing"]["ladder_fractions"] = [0.25]
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )


def test_climbing_ladder_fractions_are_finite_open_unit():
    class IntSubclass(int):
        pass

    class FloatSubclass(float):
        pass

    invalid_values = (
        0,
        0.0,
        1,
        1.0,
        -1,
        -1.0,
        2,
        2.0,
        True,
        False,
        float("nan"),
        float("inf"),
        float("-inf"),
        10**400,
        "0.25",
        None,
        [],
        {},
        IntSubclass(0),
        FloatSubclass(0.25),
    )
    for invalid in invalid_values:
        raw = load_config(Path("config.toml")).raw
        raw["exits"]["climbing"]["ladder_multiples"] = [1.5]
        raw["exits"]["climbing"]["ladder_fractions"] = [invalid]
        with pytest.raises(
            ConfigError,
            match=(
                r"exits\.climbing\.ladder_fractions.*finite.*"
                r"strictly between zero and one"
            ),
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    raw["exits"]["climbing"]["ladder_multiples"] = [1.5, 2.0]
    raw["exits"]["climbing"]["ladder_fractions"] = [0.25, float("nan")]
    with pytest.raises(
        ConfigError,
        match=(
            r"exits\.climbing\.ladder_fractions.*finite.*"
            r"strictly between zero and one"
        ),
    ):
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )

    for valid in (5e-324, 0.25, 0.9999999999999999):
        raw = load_config(Path("config.toml")).raw
        raw["exits"]["climbing"]["ladder_multiples"] = [1.5]
        raw["exits"]["climbing"]["ladder_fractions"] = [valid]
        assert (
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )
            is None
        )

    raw = load_config(Path("config.toml")).raw
    raw["exits"]["climbing"]["ladder_multiples"] = [1.5, 2.0]
    raw["exits"]["climbing"]["ladder_fractions"] = [0.4, 0.5]
    assert (
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )
        is None
    )


def test_climbing_ladder_fraction_sum_leaves_terminal_remainder():
    invalid_fractions = (
        [0.5, 0.5],
        [0.6, 0.5],
        [0.1] * 10,
    )
    assert math.fsum(invalid_fractions[-1]) == 1

    for fractions in invalid_fractions:
        raw = load_config(Path("config.toml")).raw
        raw["exits"]["climbing"]["ladder_multiples"] = [1.5] * len(fractions)
        raw["exits"]["climbing"]["ladder_fractions"] = fractions
        with pytest.raises(
            ConfigError,
            match=r"exits\.climbing\.ladder_fractions.*sum.*less than one",
        ):
            config_module.validate_runtime_config(
                Config(raw=raw, resolved_hash="not-relevant")
            )

    raw = load_config(Path("config.toml")).raw
    raw["exits"]["climbing"]["ladder_multiples"] = [1.5, 2.0]
    raw["exits"]["climbing"]["ladder_fractions"] = [0.2, 0.3]
    assert (
        config_module.validate_runtime_config(
            Config(raw=raw, resolved_hash="not-relevant")
        )
        is None
    )
