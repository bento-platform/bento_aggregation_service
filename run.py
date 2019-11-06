#!/usr/bin/env python3

import os
import tornado.ioloop


from chord_federation.app import application
from chord_federation.constants import CHORD_URL_SET

if __name__ == "__main__":
    if not CHORD_URL_SET:
        print("[CHORD Federation] No CHORD URL given, terminating...")
        exit(1)

    application.listen(int(os.environ.get("PORT", "5000")))
    tornado.ioloop.IOLoop.instance().start()
