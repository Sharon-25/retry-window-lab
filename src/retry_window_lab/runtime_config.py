"""Runtime-only connections; bootstrap connections remain in config.py."""
import os
from psycopg.conninfo import make_conninfo
from retry_window_lab.config import postgres_conninfo


def _restricted_conninfo(user_variable, password_variable):
    base = postgres_conninfo()  # Loads local environment before reading role variables.
    return make_conninfo(base, user=os.environ[user_variable], password=os.environ[password_variable])


def application_conninfo():
    return _restricted_conninfo("RWL_APP_USER", "RWL_APP_PASSWORD")


def provider_conninfo():
    return _restricted_conninfo("RWL_PROVIDER_USER", "RWL_PROVIDER_PASSWORD")
