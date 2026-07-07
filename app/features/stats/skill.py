from trueskill import TrueSkill

from app.core.config import settings


def make_ts_env() -> TrueSkill:
    return TrueSkill(
        mu=settings.ts_mu,
        sigma=settings.ts_sigma,
        beta=settings.ts_beta,
        tau=settings.ts_tau,
        draw_probability=settings.ts_draw_prob,
    )
