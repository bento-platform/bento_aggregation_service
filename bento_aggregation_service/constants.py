from __future__ import annotations

from bento_lib.service_info.helpers import build_bento_service_type
from . import __version__


__all__ = [
    "BENTO_SERVICE_KIND",
    "SERVICE_TYPE",
]

BENTO_SERVICE_KIND = "aggregation"
SERVICE_TYPE = build_bento_service_type(BENTO_SERVICE_KIND, __version__)
