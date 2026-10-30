import datetime
import importlib


def test_config_loads_env(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    # No default; the settings import fails without it.
    monkeypatch.setenv("LJ_SERVICE_TOKEN", "lj-test-token")
    monkeypatch.setenv("MONGO_URL", "mongodb://localhost:27017")
    # TrueSkill stable profile -- the five make_ts_env actually reads.
    monkeypatch.setenv("TS_MU", "1250")
    monkeypatch.setenv("TS_SIGMA", "150")
    monkeypatch.setenv("TS_BETA", "70")
    monkeypatch.setenv("TS_TAU", "1")
    monkeypatch.setenv("TS_DRAW_PROB", "0")

    import app.core.config as cfg

    importlib.reload(cfg)

    s = cfg.settings
    # not inside the assert -- pytest prints subexpressions on failure
    uri_scheme_ok = s.mongo_url.get_secret_value().startswith("mongodb://")
    assert uri_scheme_ok
    assert s.ts_mu == 1250
    assert s.ts_sigma == 150
    assert s.ts_beta == 70
    assert s.ts_tau == 1
    assert s.ts_draw_prob == 0


def test_an_auto_approve_start_without_a_zone_is_utc(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LJ_SERVICE_TOKEN", "lj-test-token")
    monkeypatch.setenv("AUTO_APPROVE_START", "2026-11-01T00:00:00")
    import app.core.config as cfg

    importlib.reload(cfg)
    start = cfg.settings.auto_approve_start
    assert start is not None and start.utcoffset() == datetime.timedelta(0)
    assert start > datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
