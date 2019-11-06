#!/usr/bin/env python3

import os
import tornado.ioloop


from chord_federation.app import application

if __name__ == "__main__":
    if os.environ.get("CHORD_URL", None) is None:
        print("[CHORD Federation] No CHORD URL given, terminating...")
        exit(1)

    application.listen(int(os.environ.get("PORT", "5000")))
    tornado.ioloop.IOLoop.instance().start()
