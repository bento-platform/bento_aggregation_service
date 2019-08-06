#!/usr/bin/env python3

import os
import tornado.ioloop


from chord_federation_async.app import application

if __name__ == "__main__":
    application.listen(int(os.environ.get("PORT", "5000")))
    tornado.ioloop.IOLoop.instance().start()
