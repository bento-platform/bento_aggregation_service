import json
import tornado.gen

from chord_lib.responses.errors import bad_request_error, forbidden_error
from datetime import datetime, timedelta
from sqlite3 import Connection
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.queues import Queue
from tornado.web import RequestHandler
from typing import Dict, List, Set

from .constants import *
from .db import check_peer_exists, insert_or_ignore_peer
from .utils import peer_fetch


class PeerManager:
    def __init__(self, db: Connection):
        self.db = db

        self.last_peers_update = datetime.utcfromtimestamp(0)
        self.peer_cache_invalidated = False
        self.connected_to_peer_network = False
        self.fetching_peers = False
        self.last_errored: Dict[str, float] = {}
        self.contacting: Set[str] = set()
        self.notifying: Set[str] = set()

    async def peer_worker(self, peers: Set[str], peers_to_check: Queue, peers_to_check_set: Set[str],
                          attempted_contact: Set[str], results: List[bool]):
        client = AsyncHTTPClient()

        async for peer in peers_to_check:
            if peer is None:
                # Exit signal
                return

            if (peer in self.last_errored and
                    datetime.now().timestamp() - self.last_errored[peer] < LAST_ERRORED_CACHE_TIME):
                # Avoid repetitively hitting dead nodes
                print("[CHORD Federation {}] Skipping dead peer {}".format(datetime.now(), peer), flush=True)
                peers_to_check_set.remove(peer)
                peers_to_check.task_done()
                continue

            if peer in attempted_contact:
                peers_to_check_set.remove(peer)
                peers_to_check.task_done()
                continue

            if peer in self.contacting:
                print("[CHORD Federation {}] Avoiding race on peer {}".format(datetime.now(), peer), flush=True)
                # TODO: Do we call task_done() here?
                continue

            self.contacting.add(peer)

            print("[CHORD Federation {}] Contacting peer {}".format(datetime.now(), peer), flush=True)

            peer_peers: List[str] = []

            try:
                # TODO: Combine requests?

                # Notify peer of current node's existence, OIDC realm, and peer list
                await peer_fetch(
                    client=client,
                    peer=peer,
                    path_fragment="api/federation/peers",  # TODO: This should probably be parametrized
                    request_body=json.dumps({
                        "peers": list(peers),
                        "self": CHORD_URL,
                        "oidc_discovery_uri": OIDC_DISCOVERY_URI,
                    })
                )

                # Fetch the peer's peer list
                r = await peer_fetch(client=client, peer=peer, path_fragment="api/federation/peers", method="GET")

                # If a non-200 response is encountered, an error is raised

                self.connected_to_peer_network = True
                peer_peers = r["peers"]

            except IndexError:
                print(f"[CHORD Federation] Error: Invalid 200 response returned by {peer}.", flush=True)

            except HTTPError as e:
                print("[CHORD Federation] Peer contact error for {} ({})".format(peer, str(e)), flush=True)
                self.last_errored[peer] = datetime.now().timestamp()

            # Incorporate the peer's peer list into the current set of peers
            peers = peers.union(peer_peers)

            # Search for new peers, and if they exist add them to the queue containing peers to verify
            new_peer = False
            for p in peer_peers:
                if p not in peers_to_check_set and p not in self.contacting and p not in attempted_contact:
                    new_peer = True
                    peers_to_check.put_nowait(p)
                    peers_to_check_set.add(p)

            results.append(new_peer)

            attempted_contact.add(peer)
            self.contacting.remove(peer)

            peers_to_check_set.remove(peer)
            peers_to_check.task_done()

    async def get_peers(self) -> Set[str]:
        c = self.db.cursor()

        c.execute("SELECT url FROM peers")
        peers: Set[str] = set(p[0] for p in c.fetchall())

        if (datetime.utcnow() - timedelta(hours=1) > self.last_peers_update or self.peer_cache_invalidated) \
                and not self.fetching_peers:
            self.fetching_peers = True
            self.last_peers_update = datetime.utcnow()

            # Peer queue
            peers_to_check = Queue()
            for p in peers:
                peers_to_check.put_nowait(p)

            peers_to_check_set: Set[str] = peers.copy()

            results: List[bool] = []
            attempted_contact: Set[str] = {CHORD_URL}

            # noinspection PyAsyncCall,PyTypeChecker
            workers = tornado.gen.multi([
                self.peer_worker(peers, peers_to_check, peers_to_check_set, attempted_contact, results)
                for _ in range(WORKERS)])

            # Wait for all peers to be processed
            await peers_to_check.join()

            self.peer_cache_invalidated = self.peer_cache_invalidated or (True in results)

            # Store any new peers in the possibly augmented set
            for peer in peers:
                insert_or_ignore_peer(c, peer)

            # Commit any new peers to the database
            self.db.commit()

            self.fetching_peers = False

            # Trigger exit for all workers
            for _ in range(WORKERS):
                peers_to_check.put_nowait(None)

            # Wait for workers to exit
            await workers

        return peers


# noinspection PyAbstractClass,PyAttributeOutsideInit
class PeerHandler(RequestHandler):
    async def options(self):
        self.set_status(204)
        await self.finish()

    async def get(self):
        peers = list(await self.application.peer_manager.get_peers())
        self.write({"peers": peers, "last_updated": self.application.peer_manager.last_peers_update.timestamp()})

    async def post(self):
        """
        Handle notifies from other nodes.
        """

        c = self.application.db.cursor()
        new_pci = self.application.peer_manager.peer_cache_invalidated

        try:
            # Test that the peer's peers can be seen and are providing the correct service type.

            request_data = json.loads(self.request.body)

            peer_oidc_discovery_uri = request_data["oidc_discovery_uri"]
            peer_self = request_data["self"]
            peer_peers = json.loads(self.request.body)["peers"]
            attempted_contact = {CHORD_URL}

            if peer_oidc_discovery_uri != OIDC_DISCOVERY_URI:
                # Difference in authentication realm, dissuade for now
                # TODO: Allow cross-realm communication
                self.clear()
                self.set_status(403)
                await self.finish(forbidden_error("OIDC realm mismatch"))
                return

            if peer_self in self.application.peer_manager.notifying:
                # Another request is already being processed from the same node. Assume the data is the same...
                # TODO: Is this a valid assumption?
                self.clear()
                self.set_status(200)  # TODO: Wrong response code?
                return

            self.application.peer_manager.notifying.add(peer_self)

            client = AsyncHTTPClient()

            for peer_url in peer_peers:
                if peer_url in attempted_contact:
                    continue

                if (peer_url in self.application.peer_manager.last_errored and
                        datetime.now().timestamp() - self.application.peer_manager.last_errored[peer_url] <
                        LAST_ERRORED_CACHE_TIME):
                    # Avoid repetitively hitting dead nodes
                    continue

                try:
                    r = await peer_fetch(
                        client=client,
                        peer=peer_url,
                        path_fragment="api/federation/service-info?update_peers=false",
                        method="GET"
                    )

                    # TODO: Check semver for compatibility
                    # TODO: Check JSON schema
                    if r["type"].startswith(f"{SERVICE_ORGANIZATION}:{SERVICE_ARTIFACT}"):
                        # Peer two-way communication is possible
                        new_pci = new_pci or not check_peer_exists(c, peer_url)
                        insert_or_ignore_peer(c, peer_url)
                        self.application.db.commit()

                except Exception as e:  # Parse error or HTTP error
                    # TODO: Better / more compliant error message, don't return early
                    self.application.peer_manager.last_errored[peer_url] = datetime.now().timestamp()
                    print("[CHORD Federation {}] Error when processing notify from peer {}.\n"
                          "    Error: {}".format(datetime.now(), peer_url, str(e)), flush=True)

                finally:
                    attempted_contact.add(peer_url)

            self.application.peer_manager.notifying.remove(peer_self)

            self.application.peer_manager.peer_cache_invalidated = new_pci
            self.clear()
            self.set_status(204)

        except IndexError:
            # TODO: Better / more compliant error message
            self.clear()
            self.set_status(400)
            await self.finish(bad_request_error("Invalid request body or other Python KeyError"))  # TODO: Better msg
