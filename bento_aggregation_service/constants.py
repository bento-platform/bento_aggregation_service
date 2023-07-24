from __future__ import annotations

import os

from . import __version__


__all__ = [
    "OIDC_DISCOVERY_URI",

    "DB_PATH",

    "MAX_BUFFER_SIZE",

    "BENTO_SERVICE_KIND",
    "SERVICE_ORGANIZATION",
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_NAME",

    "USE_GOHAN",

    "TIMEOUT",
    "LAST_ERRORED_CACHE_TIME",
]


def _env_to_bool(var: str, default: bool = False) -> bool:
    return os.environ.get(var, str(default)).strip().lower() == "true"


def _env_url_trailing_slash(var: str) -> str:
    """
    Hack in a ubiquitous trailing slash by appending it to an rstripped version
    and lstripping the / to remove it if the URL is blank.
    :param var: The environment variable for the URL value
    :return: A trailing-slash-guaranteed version, or blank.
    """
    return (os.environ.get(var, "").strip().rstrip("/") + "/").lstrip("/")


OIDC_DISCOVERY_URI = os.environ.get("OIDC_DISCOVERY_URI")

DB_PATH = os.path.join(os.getcwd(), os.environ.get("DATABASE", "data/federation.db"))

BENTO_SERVICE_KIND = "aggregation"
SERVICE_ORGANIZATION = "ca.c3g.bento"
SERVICE_ARTIFACT = BENTO_SERVICE_KIND
SERVICE_TYPE = {
    "group": "ca.c3g.bento",
    "artifact": SERVICE_ARTIFACT,
    "version": __version__,
}
SERVICE_NAME = "Bento Aggregation Service"

USE_GOHAN = _env_to_bool("USE_GOHAN")

TIMEOUT = 180  # seconds
LAST_ERRORED_CACHE_TIME = 30
MAX_BUFFER_SIZE = 1024 ** 3  # 1 gigabyte; maximum size a response can be
