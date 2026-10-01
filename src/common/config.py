"""Central place to read settings from environment variables (.env)."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.host} port={self.port} user={self.user} "
            f"password={self.password} dbname={self.dbname}"
        )


def dwh_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.getenv("DWH_HOST", "localhost"),
        port=int(os.getenv("DWH_PORT", "5434")),
        user=os.environ["DWH_POSTGRES_USER"],
        password=os.environ["DWH_POSTGRES_PASSWORD"],
        dbname=os.environ["DWH_POSTGRES_DB"],
    )


def oltp_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.getenv("OLTP_HOST", "localhost"),
        port=int(os.getenv("OLTP_PORT", "5433")),
        user=os.environ["OLTP_POSTGRES_USER"],
        password=os.environ["OLTP_POSTGRES_PASSWORD"],
        dbname=os.environ["OLTP_POSTGRES_DB"],
    )
