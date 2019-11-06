import os


__all__ = [
    "BASE_PATH",
    "CHORD_URL",
    "CHORD_REGISTRY_URL",
    "TIMEOUT",
    "WORKERS",
    "LAST_ERRORED_CACHE_TIME",
]


BASE_PATH = os.environ.get("BASE_URL", "")
CHORD_URL = os.environ.get("CHORD_URL", None)
CHORD_REGISTRY_URL = os.environ.get("CHORD_REGISTRY_URL", "http://127.0.0.1:5000/")  # "http://1.chord.dlougheed.com/"
TIMEOUT = 45
WORKERS = 10
LAST_ERRORED_CACHE_TIME = 30
