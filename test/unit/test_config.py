import importlib

def test_config_loads_env(monkeypatch):
    # minimal required env
    monkeypatch.setenv("REPORTING_BACKEND_VERSION", "test-1.2.3")
    monkeypatch.setenv("MONGO_URL", "mongodb://localhost:27017")
    # TrueSkill stable profile
    monkeypatch.setenv("TS_MU", "1250")
    monkeypatch.setenv("TS_SIGMA", "150")
    monkeypatch.setenv("TS_BETA", "70")
    monkeypatch.setenv("TS_TAU", "1")
    monkeypatch.setenv("TS_DRAW_PROB", "0")
    monkeypatch.setenv("TS_SIGMA_FREE", "90")
    monkeypatch.setenv("TS_TEAMER_BOOST", "1.0")
    # CORS
    monkeypatch.setenv("ALLOWED_ORIGINS", "http://localhost:3000,https://example.com")

    # reload after setting env
    import app.core.config as cfg
    importlib.reload(cfg)

    s = cfg.settings
    assert s.reporting_backend_version == "test-1.2.3"
    # not inside the assert -- pytest prints subexpressions on failure
    uri_scheme_ok = s.mongodb_uri.get_secret_value().startswith("mongodb://")
    assert uri_scheme_ok
    assert s.ts_mu == 1250
    assert s.ts_sigma == 150
    assert s.ts_beta == 70
    assert s.ts_tau == 1
    assert s.ts_draw_prob == 0
    assert s.ts_sigma_free == 90
    assert s.ts_teamer_boost == 1.0
    assert s.allowed_origins == ["http://localhost:3000", "https://example.com"]