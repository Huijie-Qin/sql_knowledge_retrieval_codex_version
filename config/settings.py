from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class Settings(BaseSettings):
        source_dir: Path = Path("./案例")
        output_dir: Path = Path("./数据源")
        max_context_tokens: int = 128000

        model_config = SettingsConfigDict(env_file=".env", extra="ignore")

except ImportError:

    @dataclass
    class Settings:
        source_dir: Path = Path("./案例")
        output_dir: Path = Path("./数据源")
        max_context_tokens: int = 128000

        def __init__(self) -> None:
            self.source_dir = Path(os.getenv("SOURCE_DIR", "./案例"))
            self.output_dir = Path(os.getenv("OUTPUT_DIR", "./数据源"))
            self.max_context_tokens = int(os.getenv("MAX_CONTEXT_TOKENS", "128000"))


settings = Settings()
