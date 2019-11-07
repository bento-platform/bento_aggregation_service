import json
import tornado.gen

from datetime import datetime
from itertools import chain
from tornado.httpclient import AsyncHTTPClient
from tornado.queues import Queue
from tornado.web import RequestHandler

from .constants import TIMEOUT, WORKERS


# noinspection PyAbstractClass
class SearchHandler(RequestHandler):
    async def search_worker(self, peer_queue: Queue, search_path: str, responses: list):
        client = AsyncHTTPClient()

        async for peer in peer_queue:
            if peer is None:
                # Exit signal
                return

            try:
                r = await client.fetch(f"{peer}api/{search_path}", request_timeout=TIMEOUT, method="POST",
                                       body=self.request.body, headers={"Content-Type": "application/json"},
                                       raise_error=True)
                responses.append(json.loads(r.body))

            except Exception as e:
                # TODO: Less broad of an exception
                responses.append(None)
                print("[CHORD Federation {}] Connection issue or timeout with peer {}.\n"
                      "    Error: {}".format(datetime.now(), peer, str(e)), flush=True)

            finally:
                peer_queue.task_done()

    async def post(self, search_path: str):
        # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP

        request = None

        try:
            request = json.loads(self.request.body)
        except json.JSONDecodeError:
            # TODO: Better / more compliant error message
            self.set_status(400)

        # TODO: Validate against a JSON schema or OpenAPI
        if not isinstance(request, dict):
            return

        c = self.application.db.cursor()
        peers = await self.application.peer_manager.get_peers(c)
        self.application.db.commit()

        peer_queue = Queue()
        for peer in peers:
            peer_queue.put_nowait(peer)

        responses = []
        workers = tornado.gen.multi([self.search_worker(peer_queue, search_path, responses) for _ in range(WORKERS)])
        await peer_queue.join()
        good_responses = tuple(r for r in responses if r is not None)

        try:
            self.write({
                "results": list(chain.from_iterable((r["results"] for r in good_responses))),
                "peers": {"responded": len(good_responses), "total": len(responses)}
            })

        except IndexError:
            # TODO: Better / more compliant error message
            self.clear()
            self.set_status(400)

        # Trigger exit for all workers
        for _ in range(WORKERS):
            peer_queue.put_nowait(None)

        # Wait for workers to exit
        await workers
