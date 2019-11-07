import json
import tornado.gen

from datetime import datetime, timedelta
from tornado.httpclient import AsyncHTTPClient
from tornado.queues import Queue
from tornado.web import RequestHandler
from typing import Dict, List, Set

from .constants import *
from .db import check_peer_exists, insert_or_ignore_peer


class PeerManager:
    def __init__(self):
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
                # if peers_to_check.qsize() == 0:
                #     return
                continue

            if peer in self.contacting:
                print("[CHORD Federation {}] Avoiding race on peer {}".format(datetime.now(), peer), flush=True)
                # TODO: Do we call task_done() here?
                continue

            self.contacting.add(peer)

            print("[CHORD Federation {}] Contacting peer {}".format(datetime.now(), peer), flush=True)

            peer_peers: List[str] = []

            try:
                await client.fetch(
                    f"{peer}api/federation/peers",
                    request_timeout=TIMEOUT,
                    method="POST",
                    body=json.dumps({"peers": list(peers), "self": CHORD_URL}),
                    headers={"Content-Type": "application/json"},
                    raise_error=True
                )

                r = await client.fetch(f"{peer}api/federation/peers", method="GET", request_timeout=TIMEOUT)

                # If a non-200 response is encountered, an error is raised

                self.connected_to_peer_network = True
                peer_peers = json.loads(r.body)["peers"]

            except IndexError:
                print(f"[CHORD Federation] Error: Invalid 200 response returned by {peer}.", flush=True)

            except Exception as e:
                # TODO: Less generic error
                print("[CHORD Federation] Peer contact error for {}".format(peer), flush=True)
                self.last_errored[peer] = datetime.now().timestamp()
                print(peer, str(e), flush=True)

            peers = peers.union(peer_peers)
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

    async def get_peers(self, c) -> Set[str]:
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

            self.fetching_peers = False

            # Trigger exit for all workers
            for _ in range(WORKERS):
                peers_to_check.put_nowait(None)

            # Wait for workers to exit
            await workers

        return peers


# noinspection PyAbstractClass,PyAttributeOutsideInit
class PeerHandler(RequestHandler):
    async def get(self):
        c = self.application.db.cursor()
        peers = await self.application.peer_manager.get_peers(c)

        # Commit any changes triggered by the get
        self.application.db.commit()

        self.write({"peers": list(peers), "last_updated": self.application.peer_manager.last_peers_update.timestamp()})

    async def post(self):
        """
        Handle notifies from other nodes.
        """

        c = self.application.db.cursor()
        new_pci = self.application.peer_manager.peer_cache_invalidated

        try:
            # Test that the peer's peers can be seen and are providing the correct service type.

            request_data = json.loads(self.request.body)

            peer_self = request_data["self"]
            peer_peers = json.loads(self.request.body)["peers"]
            attempted_contact = {CHORD_URL}

            if peer_self in self.application.peer_manager.notifying:
                # Another request is already being processed from the same node. Assume the data is the same...
                # TODO: Is this a valid assumption?

                self.clear()
                self.set_status(200)

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
                    r = await client.fetch(f"{peer_url}api/federation/service-info?update_peers=false",
                                           request_timeout=TIMEOUT)

                    # TODO: Check semver for compatibility
                    if "ca.distributedgenomics:chord_federation" in json.loads(r.body)["type"]:
                        # Peer two-way communication is possible
                        new_pci = new_pci or not check_peer_exists(c, peer_url)
                        insert_or_ignore_peer(c, peer_url)
                        self.application.db.commit()

                except Exception as e:
                    # TODO: Better / more compliant error message, don't return early
                    self.application.peer_manager.last_errored[peer_url] = datetime.now().timestamp()
                    print("[CHORD Federation {}] Error when processing notify from peer {}.\n"
                          "    Error: {}".format(datetime.now(), peer_url, str(e)), flush=True)

                finally:
                    attempted_contact.add(peer_url)

            self.application.peer_manager.notifying.remove(peer_self)

            self.application.peer_manager.peer_cache_invalidated = new_pci
            self.clear()
            self.set_status(200)

        except IndexError:
            # TODO: Better / more compliant error message
            self.clear()
            self.set_status(400)
