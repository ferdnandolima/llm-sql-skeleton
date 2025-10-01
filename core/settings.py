# core/settings.py
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # Onde ler variáveis e como tratá-las
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    # Ambiente
    ENV: str = Field(default="dev")

    # ---- DB (MySQL/PyMySQL via SQLAlchemy) ----
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASS: str = ""
    DB_NAME: str = ""
    DB_CHARSET: str = "utf8mb4"

    # Pool
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 5
    DB_POOL_PRE_PING: bool = True
    DB_POOL_RECYCLE: int = 1800  # segundos

    # Timeouts (segundos) – repassados via connect_args
    DB_CONNECT_TIMEOUT: int = 5
    DB_READ_TIMEOUT: int = 10
    DB_WRITE_TIMEOUT: int = 10

    # Debug SQLAlchemy
    DB_ECHO: bool = False

    # ---- Regras globais do motor ----
    GLOBAL_LIMIT_CAP: int = 1000      # teto duro para LIMIT
    MAX_ROWS_PAYLOAD: int = 5000      # truncamento do payload de linhas
    DB_STATEMENT_TIMEOUT_MS: int = 5000
    LLM_TIMEOUT_MS: int = 5000

    # ---- Cache de SELECTs ----
    CACHE_SELECT_TTL: int = 15        # 0 desliga
    CACHE_MAX_ITEMS: int = 256

@lru_cache()
def get_settings() -> Settings:
    return Settings()
