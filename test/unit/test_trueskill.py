import importlib
from trueskill import Rating


def test_trueskill_1v1_winner_gains(monkeypatch):
    # ensure env present for the config used by make_ts_env()
    monkeypatch.setenv("REPORTING_BACKEND_VERSION", "test-1.2.3")
    monkeypatch.setenv("MONGO_URI", "mongodb://localhost:27017")
    monkeypatch.setenv("TS_MU", "1250")
    monkeypatch.setenv("TS_SIGMA", "150")
    monkeypatch.setenv("TS_BETA", "70")
    monkeypatch.setenv("TS_TAU", "1")
    monkeypatch.setenv("TS_DRAW_PROB", "0")
    monkeypatch.setenv("TS_SIGMA_FREE", "90")
    monkeypatch.setenv("TS_TEAMER_BOOST", "1.0")

    # reload modules to pick up env
    import app.core.config as cfg

    importlib.reload(cfg)
    import app.features.ratings.skill as skill

    importlib.reload(skill)

    env = skill.make_ts_env()
    mu, sigma = cfg.settings.ts_mu, cfg.settings.ts_sigma
    r1, r2 = Rating(mu, sigma), Rating(mu, sigma)

    (w,), (loser,) = env.rate([[r1], [r2]], ranks=[1, 2])

    assert w.mu > r1.mu, "Winner's mu should increase"
    assert loser.mu < r2.mu, "Loser's mu should decrease"
