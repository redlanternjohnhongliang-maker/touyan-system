from dataclasses import dataclass
import os
from pathlib import Path


@dataclass
class Settings:
    gemini_api_key: str
    db_path: str


def load_settings() -> Settings:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    default_db_path = Path(__file__).resolve().parents[2] / "data" / "app.db"
    db_path = os.environ.get("APP_DB_PATH", str(default_db_path)).strip()
    return Settings(gemini_api_key=api_key, db_path=db_path)
