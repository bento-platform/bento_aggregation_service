import os
import urllib.parse

from . import __version__


__all__ = [
    "BASE_PATH",
    "CHORD_DEBUG",
    "CHORD_URL",
    "CHORD_HOST",

    "OIDC_DISCOVERY_URI",

    "DB_PATH",

    "MAX_BUFFER_SIZE",

    "SERVICE_ORGANIZATION",
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_ID",
    "SERVICE_NAME",

    "PORT",
    "DEBUGGER_PORT",

    "CHORD_URL_SET",

    "USE_GOHAN",

    "TIMEOUT",
    "WORKERS",
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


# Should usually be blank; set to non-blank to locally emulate a proxy prefix
# like /api/federation
BASE_PATH = os.environ.get("SERVICE_URL_BASE_PATH", "")

CHORD_DEBUG = _env_to_bool("CHORD_DEBUG")

# Set CHORD_URL and CHORD_REGISTRY_URL to environment values, or blank if not
# available.
CHORD_URL = _env_url_trailing_slash("CHORD_URL")

CHORD_HOST = urllib.parse.urlparse(CHORD_URL or "").netloc or ""
OIDC_DISCOVERY_URI = os.environ.get("OIDC_DISCOVERY_URI")

DB_PATH = os.path.join(os.getcwd(), os.environ.get("DATABASE", "data/federation.db"))

SERVICE_ORGANIZATION = "ca.c3g.bento"
SERVICE_ARTIFACT = "federation"
SERVICE_TYPE_NO_VERSION = f"{SERVICE_ORGANIZATION}:{SERVICE_ARTIFACT}"
SERVICE_TYPE = f"{SERVICE_TYPE_NO_VERSION}:{__version__}"
SERVICE_ID = os.environ.get("SERVICE_ID", SERVICE_TYPE_NO_VERSION)
SERVICE_NAME = "Bento Federation Service"

PORT = int(os.environ.get("PORT", "5000"))
DEBUGGER_PORT = int(os.environ.get("DEBUGGER_PORT", "5879"))

CHORD_URL_SET = CHORD_URL != ""

USE_GOHAN = _env_to_bool("USE_GOHAN")

TIMEOUT = 180  # seconds
LAST_ERRORED_CACHE_TIME = 30
MAX_BUFFER_SIZE = 1024 ** 3  # 1 gigabyte; maximum size a response can be

try:
    # noinspection PyUnresolvedReferences
    WORKERS = len(os.sched_getaffinity(0))
except AttributeError:  # Some operating systems don't provide sched_getaffinity
    WORKERS = 4  # Default to 4 workers
