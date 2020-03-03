#!/usr/bin/env python3

import os
import tornado.ioloop

from chord_federation_service.app import application
from chord_federation_service.constants import CHORD_URLS_SET, SERVICE_NAME

if __name__ == "__main__":
    if not CHORD_URLS_SET:
        print(f"[{SERVICE_NAME}] No CHORD URLs given, terminating...")
        exit(1)

    application.listen(int(os.environ.get("PORT", "5000")))
    tornado.ioloop.IOLoop.instance().start()
