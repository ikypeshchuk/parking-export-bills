

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    name: str
    user: str
    password: str
    host: str
    port: int


@dataclass
class AppConfig:
    """Application configuration settings."""
    batch_limit: int
    task_interval: int
    db_config: DatabaseConfig
    sqlite_path: Path
