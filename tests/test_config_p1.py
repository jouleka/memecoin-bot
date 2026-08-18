from pathlib import Path

from memebot.config import load_config


def test_p1_sections_present_and_typed():
    cfg = load_config(Path("config.toml"))
    strat = cfg.section("strategy")["climbing"]
    assert strat["score_threshold"] >= 50.0
    assert strat["position_size_sol"] > 0
    assert strat["max_concurrent_positions"] >= 1
    assert strat["min_samples"] >= 2            # excludes the instant-graduation insider tail

    weights = cfg.section("scorer")["climbing"]
    assert weights["weights_version"]           # non-empty string, stamped on every decision
    assert weights["w_velocity"] >= weights["w_progress"]   # velocity-dominant (evidence #1 signal)

    fill = cfg.section("fill")
    assert fill["latency_min_s"] >= 1.0         # T: fill from a snapshot >= T after the decision

    exits = cfg.section("exits")["climbing"]
    assert len(exits["ladder_multiples"]) == len(exits["ladder_fractions"])
    assert sum(exits["ladder_fractions"]) < 1.0  # leaves a moon-bag
    assert exits["time_stop_s"] > 0

    assert cfg.section("paper")["bankroll_sol"] > 0

    counterfactual = cfg.section("counterfactual")
    assert counterfactual["stale_price_after_s"] > 0   # off-poll age-out bound (N2 fix)


def test_config_hash_is_stable():
    a = load_config(Path("config.toml")).resolved_hash
    b = load_config(Path("config.toml")).resolved_hash
    assert a == b and len(a) == 64
