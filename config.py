from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    database_url: str
    echo_api_base: str = "https://echo.epa.gov/Rest/api"
    echo_user_agent: str = "echoHazards/1.0"
    nrc_data_dir: str = "./data/nrc"
    echo_data_dir: str = "./data/echo"
    tri_data_dir: str = "./data/tri"
    ingest_batch_size: int = 500
    echo_rate_limit_delay: float = 1.0
    log_level: str = "INFO"
    environment: str = "development"

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()
