import json
import os

from . import __version__


__all__ = [
    "BASE_PATH",
    "CHORD_URL",
    "CHORD_REGISTRY_URL",
    "CHORD_SERVICES",

    "DB_PATH",

    "SERVICE_TYPE",
    "SERVICE_ID",
    "SERVICE_SOCKET",

    "SOCKET_FORMAT",

    "CHORD_URLS_SET",

    "TIMEOUT",
    "WORKERS",
    "LAST_ERRORED_CACHE_TIME",
]


BASE_PATH = os.environ.get("SERVICE_URL_BASE_PATH", "")
CHORD_URL = os.environ.get("CHORD_URL", "")
CHORD_REGISTRY_URL = os.environ.get("CHORD_REGISTRY_URL", "")  # "http://1.chord.dlougheed.com/"
CHORD_SERVICES_PATH = os.environ.get("CHORD_SERVICES", "chord_services.json")
CHORD_SERVICES = json.load(open(CHORD_SERVICES_PATH, "r"))
DB_PATH = os.path.join(os.getcwd(), os.environ.get("DATABASE", "data/federation.db"))
SERVICE_TYPE = "ca.c3g.chord:federation:{}".format(__version__)
SERVICE_ID = os.environ.get("SERVICE_ID", SERVICE_TYPE)
SERVICE_SOCKET = os.environ.get("SERVICE_SOCKET", "/tmp/federation.sock")
SOCKET_FORMAT = os.environ.get("SOCKET_FORMAT", "/chord/tmp/{artifact}.sock")  # for other services

CHORD_URLS_SET = CHORD_URL.strip() != "" and CHORD_REGISTRY_URL.strip() != ""

TIMEOUT = 120
WORKERS = 10
LAST_ERRORED_CACHE_TIME = 30
