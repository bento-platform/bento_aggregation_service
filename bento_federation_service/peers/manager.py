import json
import sys
import tornado.gen

from datetime import datetime, timedelta
from sqlite3 import Connection
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.queues import Queue
from typing import Dict, List, Set

from ..constants import CHORD_URL, LAST_ERRORED_CACHE_TIME, OIDC_DISCOVERY_URI, SERVICE_NAME, WORKERS
from ..db import insert_or_ignore_peer
from ..utils import peer_fetch


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
                print(f"[{SERVICE_NAME} {datetime.now()}] Skipping dead peer {peer}", flush=True)
                peers_to_check_set.remove(peer)
                peers_to_check.task_done()
                continue

            if peer in attempted_contact:
                peers_to_check_set.remove(peer)
                peers_to_check.task_done()
                continue

            if peer in self.contacting:
                print(f"[{SERVICE_NAME} {datetime.now()}] Avoiding race on peer {peer}", flush=True)
                # TODO: Do we call task_done() here?
                continue

            self.contacting.add(peer)

            print(f"[{SERVICE_NAME} {datetime.now()}] Contacting peer {peer}", flush=True)

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
                print(f"[{SERVICE_NAME} {datetime.now()}] [ERROR] Invalid 200 response returned by {peer}.", flush=True,
                      file=sys.stderr)

            except (HTTPError, ValueError) as e:
                # HTTPError:  Standard 400s/500s
                # ValueError: ex. Unsupported url scheme: api/federation/peers
                now = datetime.now()
                print(f"[{SERVICE_NAME} {now}] [ERROR] Peer contact error for {peer} ({str(e)})", flush=True,
                      file=sys.stderr)
                self.last_errored[peer] = now.timestamp()

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
