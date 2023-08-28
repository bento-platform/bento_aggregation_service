from __future__ import annotations

from . import __version__


__all__ = [
    "BENTO_SERVICE_KIND",
    "SERVICE_ORGANIZATION",
    "SERVICE_ARTIFACT",
    "SERVICE_TYPE",
    "SERVICE_NAME",
]

BENTO_SERVICE_KIND = "aggregation"
SERVICE_ORGANIZATION = "ca.c3g.bento"
SERVICE_ARTIFACT = BENTO_SERVICE_KIND
SERVICE_TYPE = {
    "group": "ca.c3g.bento",
    "artifact": SERVICE_ARTIFACT,
    "version": __version__,
}
SERVICE_NAME = "Bento Aggregation Service"
