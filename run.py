#!/usr/bin/env python3

import os
import tornado.ioloop

from bento_aggregation_service.app import application
from bento_aggregation_service.constants import CHORD_URLS_SET, SERVICE_NAME, CHORD_DEBUG

if __name__ == "__main__":
    if not CHORD_URLS_SET:
        print(f"[{SERVICE_NAME}] No CHORD URLs given, terminating...")
        exit(1)

    print(f"[{SERVICE_NAME}] Started")
    if CHORD_DEBUG:
        try:
            import debugpy
            DEBUGGER_PORT = int(os.environ.get("DEBUGGER_PORT", "5879"))
            debugpy.listen(("0.0.0.0", DEBUGGER_PORT))
            print('Debugger Attached')
        except ImportError:
            print('Library debugpy not found.')

    application.listen(int(os.environ.get("PORT", "5000")))
    tornado.ioloop.IOLoop.instance().start()
