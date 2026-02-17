from fastapi import APIRouter
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # DB
    DATABASE_URL: str
    PG_STATEMENT_TIMEOUT_MS: int = 3
    # LLM

    LLM_PROVIDER: str = "openai"  # ollama | openai

    # OpenAI
    OPENAI_API_KEY: str | None = None
    OPENAI_BASE_URL: str | None = None  # optional (for proxies)
    OPENAI_MODEL: str = "gpt-4o-mini"

    DEFAULT_LLM_MODEL: str = "ministral-3:8B"
    DEFAULT_TEMPERATURE: float = 0.6
    OLLAMA_BASE_URL: str = "http://127.0.0.1:11434"

    # RAG SERVICE
    RAGDB_URL: str = "http://127.0.0.1:9000"

    # App
    APP_NAME: str = "LLM Orchestrator"
    ENV: str = "dev"



    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

settings = Settings()


config_router = APIRouter()


@config_router.get("/config")
def get_config():
    return {
        "database_url": settings.DATABASE_URL,
        "default_model": settings.DEFAULT_LLM_MODEL,
        "default_temperature": settings.DEFAULT_TEMPERATURE,
        "ollama_base_url": settings.OLLAMA_BASE_URL,
        "env": settings.ENV,
    }