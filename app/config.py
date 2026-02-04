from typing import List
from urllib.parse import urlsplit
from pydantic import AnyHttpUrl, Field, SecretStr, TypeAdapter, model_validator
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # MongoDB
    mongo_url: SecretStr = Field(..., env="MONGO_URL")
    mongo_db_name: str = Field(..., env="MONGO_DB_NAME")
    # MongoDB connection settings
    mongodb_timeout_ms: int = Field(5000, ge=1000, le=30000, env="MONGODB_TIMEOUT_MS")
    mongodb_max_pool_size: int = Field(100, ge=1, le=500, env="MONGODB_MAX_POOL_SIZE")
    mongodb_min_pool_size: int = Field(0, ge=0, le=100, env="MONGODB_MIN_POOL_SIZE")

    # API
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8000, gt=0, lt=65536, env="API_PORT")
    # api_port: int = Field(8001, gt=0, lt=65536, env="API_PORT")

    # CORS
    allowed_origins_raw: str = Field("http://localhost:3000", env="ALLOWED_ORIGINS")

    # TrueSkill Environment
    ts_mu: float = Field(1250.0, gt=0, env="TS_MU")
    ts_sigma: float = Field(150.0, gt=0, env="TS_SIGMA")
    ts_beta: float = Field(70.0, gt=0, env="TS_BETA")
    ts_tau: float = Field(1.0, ge=0, env="TS_TAU")
    ts_draw_prob: float = Field(0.0, ge=0, le=1, env="TS_DRAW_PROB")

    ts_sigma_free: float = Field(90.0, ge=0, env="TS_SIGMA_FREE")
    ts_teamer_boost: float = Field(1.0, env="TS_TEAMER_BOOST")
    min_points_for_subs: int = Field(5, ge=0, env="MIN_POINTS_FOR_SUBS")
    civ_save_parser_version: str = Field("1.0", env="CIV_SAVE_PARSER_VERSION")

    # pydantic v2 model config
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }

    @property
    def allowed_origins(self) -> List[str]:
        raw = (self.allowed_origins_raw or "").strip()
        items = [u.strip() for u in raw.split(",") if u.strip()] if raw else []

        adapter = TypeAdapter(List[AnyHttpUrl])
        urls = adapter.validate_python(items)

        origins: List[str] = []
        seen: set[str] = set()
        for u in urls:
            parts = urlsplit(str(u))
            origin = f"{parts.scheme}://{parts.netloc}"
            if origin not in seen:
                seen.add(origin)
                origins.append(origin)
        return origins

    @model_validator(mode="after")
    def _ensure_mongo_uri_scheme(self):
        uri = self.mongo_url
        if uri and not uri.get_secret_value().startswith(("mongodb://", "mongodb+srv://")):
            raise ValueError("MONGO_URL must start with 'mongodb://' or 'mongodb+srv://'")
        return self

settings = Settings()