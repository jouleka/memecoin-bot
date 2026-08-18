"""ConfluenceScorer (spec §5.4): deterministic, config-pinned weighted blend of the
CLIMBING feature vector into a 0-100 score. Weights are a hypothesis (velocity-dominant
per arXiv 2602.14860); re-fit from outcomes in P5. weights_version is stamped on every
decision so any outcome is attributable to the exact weights that produced it."""
from __future__ import annotations

from dataclasses import dataclass

from memebot.features import ClimbingFeatures


@dataclass(frozen=True, slots=True)
class Score:
    segment: str
    value: float
    feature_vector: dict
    weights_version: str


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


class ConfluenceScorer:
    def __init__(self, cfg: dict) -> None:
        self._cfg = cfg
        self._weights_version = cfg.get("weights_version", "")

    @property
    def weights_version(self) -> str:
        if type(self._weights_version) is not str or not self._weights_version:
            raise ValueError("weights_version must be a non-empty string")
        return self._weights_version

    def score(self, features: ClimbingFeatures, *, segment: str = "CLIMBING") -> Score:
        c = self._cfg
        n_vel = _clamp01(features.velocity_sol_per_s / c["velocity_full_scale_sol_per_s"])
        n_prog = _clamp01(features.curve_progress_pct / c["progress_full_scale_pct"])
        n_age = _clamp01(features.age_s / c["age_full_scale_s"])
        n_risk = _clamp01(1.0 - features.risk_score / 100.0)   # lower risk -> higher contribution
        n_smart = _clamp01((0.25 * features.smart_money_count)
                           + (0.75 * features.smart_money_pnl_sol
                              / c.get("smart_money_quality_full_scale_sol", 5.0)))
        w_smart = c.get("w_smart_money", 0.0)
        w_sum = c["w_velocity"] + c["w_progress"] + c["w_age"] + c["w_risk"] + w_smart
        blended = (c["w_velocity"] * n_vel + c["w_progress"] * n_prog
                   + c["w_age"] * n_age + c["w_risk"] * n_risk + w_smart * n_smart) / w_sum
        vector = {
            "velocity_sol_per_s": features.velocity_sol_per_s,
            "curve_progress_pct": features.curve_progress_pct,
            "age_s": features.age_s,
            "risk_score": features.risk_score,
            "spot_price_sol": features.spot_price_sol,
            "samples": features.samples,
            "smart_money_count": features.smart_money_count,
            "smart_money_pnl_sol": features.smart_money_pnl_sol,
        }
        return Score(segment=segment, value=100.0 * blended, feature_vector=vector,
                     weights_version=self.weights_version)
