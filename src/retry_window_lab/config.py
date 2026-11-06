import os
from pathlib import Path

from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_environment() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)


def postgres_conninfo() -> str:
    load_environment()
    return make_conninfo(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=5,
    )


def redis_url() -> str:
    load_environment()
    return os.environ["REDIS_URL"]
