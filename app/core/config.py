from __future__ import annotations

from functools import lru_cache
from typing import List
from urllib.parse import urlsplit

from pydantic import AliasChoices, AnyHttpUrl, Field, SecretStr, TypeAdapter, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Metadata
    reporting_backend_version: str = Field(
        default="dev",
        validation_alias=AliasChoices("REPORTING_BACKEND_VERSION"),
    )

    # MongoDB
    mongo_url: SecretStr = Field(
        default=SecretStr("mongodb://localhost:27017"),
        validation_alias=AliasChoices("MONGO_URL", "MONGO_URI"),
    )
    mongo_db_name: str = Field(
        default="match_reporter",
        validation_alias=AliasChoices("MONGO_DB_NAME", "MONGO_DB"),
    )
    mongodb_timeout_ms: int = Field(
        default=5000,
        ge=1000,
        le=30000,
        validation_alias=AliasChoices("MONGODB_TIMEOUT_MS"),
    )
    mongodb_max_pool_size: int = Field(
        default=100,
        ge=1,
        le=500,
        validation_alias=AliasChoices("MONGODB_MAX_POOL_SIZE"),
    )
    mongodb_min_pool_size: int = Field(
        default=0,
        ge=0,
        le=100,
        validation_alias=AliasChoices("MONGODB_MIN_POOL_SIZE"),
    )

    # API
    api_host: str = Field(default="0.0.0.0", validation_alias=AliasChoices("API_HOST"))
    api_port: int = Field(default=8000, gt=0, lt=65536, validation_alias=AliasChoices("API_PORT"))

    # CORS
    allowed_origins_raw: str = Field(
        default="http://localhost:3000",
        validation_alias=AliasChoices("ALLOWED_ORIGINS"),
    )

    # Auth parameters
    auth_service_token: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("AUTH_SERVICE_TOKEN"),
    )
    auth_session_ttl_minutes: int = Field(
        default=15,
        ge=5,
        le=120,
        validation_alias=AliasChoices("AUTH_SESSION_TTL_MINUTES"),
    )
    auth_oauth_timeout_seconds: int = Field(
        default=15,
        ge=5,
        le=60,
        validation_alias=AliasChoices("AUTH_OAUTH_TIMEOUT_SECONDS"),
    )
    auth_discord_client_id: str = Field(
        default="",
        validation_alias=AliasChoices("AUTH_DISCORD_CLIENT_ID", "DISCORD_CLIENT_ID"),
    )
    auth_discord_client_secret: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("AUTH_DISCORD_CLIENT_SECRET", "DISCORD_CLIENT_SECRET"),
    )
    auth_discord_redirect_uri: str = Field(
        default="",
        validation_alias=AliasChoices("AUTH_DISCORD_REDIRECT_URI", "DISCORD_REDIRECT_URI"),
    )
    auth_steam_api_key: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("AUTH_STEAM_API_KEY", "STEAM_API_KEY"),
    )
    auth_steam_timeout_seconds: int = Field(
        default=15,
        ge=5,
        le=60,
        validation_alias=AliasChoices("AUTH_STEAM_TIMEOUT_SECONDS"),
    )
    auth_steam_civ6_app_id: int = Field(
        default=289070,
        gt=0,
        validation_alias=AliasChoices("AUTH_STEAM_CIV6_APP_ID"),
    )
    auth_steam_civ7_app_id: int = Field(
        default=1295660,
        gt=0,
        validation_alias=AliasChoices("AUTH_STEAM_CIV7_APP_ID"),
    )
    auth_steam_civ6_required_minutes: int = Field(
        default=2880,
        ge=0,
        validation_alias=AliasChoices("AUTH_STEAM_CIV6_REQUIRED_MINUTES"),
    )
    auth_steam_civ7_required_minutes: int = Field(
        default=120,
        ge=0,
        validation_alias=AliasChoices("AUTH_STEAM_CIV7_REQUIRED_MINUTES"),
    )

    # Team gen parameters
    team_gen_tries: int = Field(default=10, gt=0, validation_alias=AliasChoices("TEAM_GEN_TRIES"))
    team_gen_randomness: float = Field(
        default=0.05,
        lt=0.1,
        gt=0,
        validation_alias=AliasChoices("TEAM_GEN_RANDOMNESS"),
    )

    # TrueSkill Environment
    ts_mu: float = Field(default=1250.0, gt=0, validation_alias=AliasChoices("TS_MU"))
    ts_sigma: float = Field(default=150.0, gt=0, validation_alias=AliasChoices("TS_SIGMA"))
    ts_beta: float = Field(default=70.0, gt=0, validation_alias=AliasChoices("TS_BETA"))
    ts_tau: float = Field(default=1.0, ge=0, validation_alias=AliasChoices("TS_TAU"))
    ts_draw_prob: float = Field(default=0.0, ge=0, le=1, validation_alias=AliasChoices("TS_DRAW_PROB"))
    ts_sigma_free: float = Field(default=90.0, ge=0, validation_alias=AliasChoices("TS_SIGMA_FREE"))
    ts_teamer_boost: float = Field(default=1.0, validation_alias=AliasChoices("TS_TEAMER_BOOST"))
    min_points_for_subs: int = Field(default=5, ge=0, validation_alias=AliasChoices("MIN_POINTS_FOR_SUBS"))
    civ_save_parser_version: str = Field(
        default="1.0",
        validation_alias=AliasChoices("CIV_SAVE_PARSER_VERSION"),
    )

    @computed_field(return_type=List[str])
    @property
    def allowed_origins(self) -> List[str]:
        raw = (self.allowed_origins_raw or "").strip()
        items = [u.strip() for u in raw.split(",") if u.strip()] if raw else []
        if not items:
            return []

        adapter = TypeAdapter(List[AnyHttpUrl])
        try:
            urls = adapter.validate_python(items)
        except Exception:
            return []

        origins: List[str] = []
        seen: set[str] = set()
        for url in urls:
            parts = urlsplit(str(url))
            origin = f"{parts.scheme}://{parts.netloc}"
            if origin not in seen:
                seen.add(origin)
                origins.append(origin)
        return origins

    @property
    def mongodb_uri(self) -> SecretStr:
        """Backward-compatible alias used by older tests and tooling."""
        return self.mongo_url

    @model_validator(mode="after")
    def _ensure_mongo_uri_scheme(self) -> "Settings":
        uri = self.mongo_url.get_secret_value()
        if not uri.startswith(("mongodb://", "mongodb+srv://")):
            raise ValueError("MONGO_URL must start with 'mongodb://' or 'mongodb+srv://'")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
