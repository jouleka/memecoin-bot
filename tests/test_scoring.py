import pytest

from memebot.features import ClimbingFeatures
from memebot.scoring import ConfluenceScorer, Score

CFG = {"weights_version": "climbing-v1", "w_velocity": 0.55, "w_progress": 0.20,
       "w_age": 0.05, "w_risk": 0.20, "velocity_full_scale_sol_per_s": 0.05,
       "progress_full_scale_pct": 80.0, "age_full_scale_s": 600.0}
SMART_CFG = {**CFG, "w_smart_money": 0.15, "smart_money_quality_full_scale_sol": 5.0}


def _feat(velocity, progress=40.0, age=300.0, risk=10.0):
    return ClimbingFeatures(velocity_sol_per_s=velocity, curve_progress_pct=progress,
                            age_s=age, risk_score=risk, spot_price_sol=1e-6, samples=5)


def test_score_in_range_and_stamps_version_and_vector():
    s = ConfluenceScorer(CFG).score(_feat(0.03))
    assert isinstance(s, Score)
    assert 0.0 <= s.value <= 100.0
    assert s.weights_version == "climbing-v1"
    assert s.feature_vector["velocity_sol_per_s"] == 0.03
    assert s.feature_vector["spot_price_sol"] == 1e-6


def test_score_is_monotonic_in_velocity():
    sc = ConfluenceScorer(CFG)
    lo = sc.score(_feat(0.005)).value
    hi = sc.score(_feat(0.05)).value
    assert hi > lo


def test_lower_risk_scores_higher():
    sc = ConfluenceScorer(CFG)
    clean = sc.score(_feat(0.03, risk=0.0)).value
    risky = sc.score(_feat(0.03, risk=100.0)).value
    assert clean > risky


def test_velocity_saturates_at_full_scale():
    sc = ConfluenceScorer(CFG)
    at_scale = sc.score(_feat(0.05)).value
    over_scale = sc.score(_feat(0.5)).value      # 10x over full scale -> clamped, equal
    assert over_scale == pytest.approx(at_scale)


def test_smart_money_presence_increases_score_and_is_in_vector():
    sc = ConfluenceScorer(SMART_CFG)
    plain = sc.score(_feat(0.02)).value
    boosted = sc.score(ClimbingFeatures(
        velocity_sol_per_s=0.02,
        curve_progress_pct=40.0,
        age_s=300.0,
        risk_score=10.0,
        spot_price_sol=1e-6,
        samples=5,
        smart_money_count=2,
        smart_money_pnl_sol=3.0,
    ))
    assert boosted.value > plain
    assert boosted.feature_vector["smart_money_count"] == 2
    assert boosted.feature_vector["smart_money_pnl_sol"] == 3.0
