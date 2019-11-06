import os


__all__ = [
    "CHORD_URL",
    "CHORD_REGISTRY_URL",
    "TIMEOUT",
    "WORKERS",
    "LAST_ERRORED_CACHE_TIME",
]


CHORD_URL = os.environ.get("CHORD_URL", "http://127.0.0.1:5000/")
CHORD_REGISTRY_URL = os.environ.get("CHORD_REGISTRY_URL", "http://127.0.0.1:5000/")  # "http://1.chord.dlougheed.com/"
TIMEOUT = 45
WORKERS = 10
LAST_ERRORED_CACHE_TIME = 30
