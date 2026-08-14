"""Settings parse from the environment.

Hermetic: without monkeypatch.chdir this reads the repo's real .env, so it
passed locally for reasons unrelated to the env it sets and behaved differently
in CI. That also hid a hard dependency -- LJ_SERVICE_TOKEN has no default and
was being supplied by the file on disk. §4 item 36.

The ts_sigma_free / ts_teamer_boost assertions are gone with the fields: they
could only fail when the dead config was removed, which is the correct change.
D55a, D86 Rule 1.
"""

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
    monkeypatch.setenv("ALLOWED_ORIGINS", "http://localhost:3000,https://example.com")

    import app.core.config as cfg

    importlib.reload(cfg)

    s = cfg.settings
    # not inside the assert -- pytest prints subexpressions on failure
    uri_scheme_ok = s.mongodb_uri.get_secret_value().startswith("mongodb://")
    assert uri_scheme_ok
    assert s.ts_mu == 1250
    assert s.ts_sigma == 150
    assert s.ts_beta == 70
    assert s.ts_tau == 1
    assert s.ts_draw_prob == 0
    assert s.allowed_origins == ["http://localhost:3000", "https://example.com"]
