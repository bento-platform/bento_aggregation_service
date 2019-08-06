import chord_federation_async
import json
import os
import sqlite3
import tornado.web

from datetime import datetime, timedelta
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.queues import Queue
from tornado.web import RequestHandler, url

CHORD_URL = os.environ.get("CHORD_URL")
TIMEOUT = 30

db_path = os.path.join(os.getcwd(), os.environ.get("DATABASE", "data/federation.db"))

db_exists = os.path.exists(db_path)
peer_db = sqlite3.connect(os.environ.get("DATABASE", "data/federation.db"), detect_types=sqlite3.PARSE_DECLTYPES)
peer_db.row_factory = sqlite3.Row


def init_db():
    with open(os.path.join("chord_federation_async", "schema.sql"), "r") as sf:
        peer_db.executescript(sf.read())

    c = peer_db.cursor()
    c.execute("INSERT OR IGNORE INTO peers VALUES(?)", (CHORD_URL,))
    # c.execute("INSERT OR IGNORE INTO peers VALUES(?)", ("http://1.chord.dlougheed.com/",))
    c.execute("INSERT OR IGNORE INTO peers VALUES(?)", ("http://127.0.0.1:5000/",))

    peer_db.commit()


def update_db():
    c = peer_db.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='peers'")
    if c.fetchone() is None:
        init_db()
        return

    # TODO


if not db_exists:
    init_db()
else:
    update_db()


# noinspection PyAbstractClass,PyAttributeOutsideInit
class ServiceInfoHandler(RequestHandler):
    async def get(self):
        # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info

        # Hack to force lists to update when the CHORD dashboard is loaded
        c = self.application.db.cursor()
        await self.application.get_peers(c)
        self.application.db.commit()

        self.write({
            "id": "ca.distributedgenomics.chord_federation",  # TODO: Should be globally unique
            "name": "CHORD Federation",  # TODO: Should be globally unique
            "type": "urn:chord:federation",  # TODO
            "description": "Federation service for a CHORD application.",
            "organization": "GenAP",
            "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
            "version": chord_federation_async.__version__,
            "extension": {}
        })


# noinspection PyAbstractClass,PyAttributeOutsideInit
class PeerHandler(RequestHandler):
    async def get(self):
        c = self.application.db.cursor()
        peers = await self.application.get_peers(c)

        # Commit any changes triggered by the get
        self.application.db.commit()

        self.write({"peers": list(peers), "last_updated": self.application.last_peers_update.timestamp()})

    async def post(self):
        c = self.application.db.cursor()
        new_pci = self.application.peer_cache_invalidated

        try:
            # Test that the peer's peers can be seen and are providing the correct service type.

            peer_peers = json.loads(self.request.body)["peers"]
            contacted = {CHORD_URL}

            client = AsyncHTTPClient()

            for peer_url in peer_peers:
                if peer_url in contacted:
                    continue

                try:
                    r = await client.fetch(HTTPRequest(url=f"{peer_url}api/federation/service-info",
                                                       request_timeout=TIMEOUT))
                    contacted.add(peer_url)

                    if json.loads(r.body)["type"] == "urn:chord:federation":
                        # Peer two-way communication is possible
                        c.execute("SELECT 1 FROM peers WHERE url = ?", (peer_url,))
                        new_pci = new_pci or c.fetchone() is None
                        c.execute("INSERT OR IGNORE INTO peers VALUES(?)", (peer_url,))
                        self.application.db.commit()

                except Exception as e:
                    # TODO: Better / more compliant error message, don't return early
                    print(str(e))
                    self.clear()
                    self.set_status(400)

            # client.close()

            self.application.peer_cache_invalidated = new_pci
            self.clear()
            self.set_status(200)

        except IndexError:
            # TODO: Better / more compliant error message
            self.clear()
            self.set_status(400)


# noinspection PyAbstractClass,PyAttributeOutsideInit
class SearchHandler(RequestHandler):
    def get(self):
        pass


class Application(tornado.web.Application):
    async def peer_worker(self, peers, peers_to_check, peers_to_check_set):
        contacted = {CHORD_URL}
        client = AsyncHTTPClient()

        async for peer in peers_to_check:
            peers_to_check_set.remove(peer)

            peer_peers = []

            if peer in contacted:
                peers_to_check.task_done()
                if peers_to_check.qsize() == 0:
                    client.close()
                    return
                continue

            try:
                await client.fetch(
                    f"{peer}api/federation/peers",
                    request_timeout=TIMEOUT,
                    method="POST",
                    body=json.dumps({"peers": list(peers)}),
                    headers={"Content-Type": "application/json"},
                    raise_error=True
                )

                r = await client.fetch(f"{peer}api/federation/peers",
                                       method="GET",
                                       request_timeout=TIMEOUT)

                # If a non-200 response is encountered, an error is raised

                self.connected_to_peer_network = True
                contacted.add(peer)
                peer_peers = json.loads(r.body)["peers"]

            except IndexError:
                print(f"Error: Invalid 200 response returned by {peer}.")

            except Exception as e:
                # TODO: Less generic error
                print(str(e))

            peers = peers.union(peer_peers)
            for p in peer_peers:
                if p not in peers_to_check_set and p not in contacted:
                    await peers_to_check.put(p)
                    peers_to_check_set.add(p)

            peers_to_check.task_done()
            if peers_to_check.qsize() == 0:
                client.close()
                return

    def __init__(self, db):
        self.db = db
        self.last_peers_update = datetime.utcfromtimestamp(0)
        self.peer_cache_invalidated = False
        self.connected_to_peer_network = False

        handlers = [
            url(r"/api/federation/service-info", ServiceInfoHandler),
            # url(r"/service-info", ServiceInfoHandler),
            url(r"/api/federation/peers", PeerHandler),
            # url(r"/peers", PeerHandler),
        ]

        super(Application, self).__init__(handlers)

    async def get_peers(self, c):
        c.execute("SELECT url FROM peers")
        peers = set([p[0] for p in c.fetchall()])

        if datetime.utcnow() - timedelta(hours=1) > self.last_peers_update or self.peer_cache_invalidated:
            self.last_peers_update = datetime.utcnow()

            # Peer queue
            peers_to_check = Queue()
            peers_to_check_set = set()
            for p in peers:
                await peers_to_check.put(p)
                peers_to_check_set.add(p)

            # noinspection PyAsyncCall,PyTypeChecker
            # await peers_to_check.join(timeout=timedelta(seconds=TIMEOUT*2))
            self.peer_cache_invalidated = self.peer_cache_invalidated or \
                await self.peer_worker(peers, peers_to_check, peers_to_check_set)

        for peer in peers:
            c.execute("INSERT OR IGNORE INTO peers VALUES (?)", (peer,))

        return peers


application = Application(peer_db)
