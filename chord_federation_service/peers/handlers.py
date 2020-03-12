import json

from chord_lib.responses.errors import bad_request_error, forbidden_error
from datetime import datetime
from tornado.httpclient import AsyncHTTPClient
from tornado.web import RequestHandler

from ..constants import (
    CHORD_URL,
    LAST_ERRORED_CACHE_TIME,
    OIDC_DISCOVERY_URI,
    SERVICE_ORGANIZATION,
    SERVICE_ARTIFACT,
    SERVICE_NAME,
)
from ..db import check_peer_exists, insert_or_ignore_peer, clear_db_and_insert_fixed_nodes
from ..utils import peer_fetch


__all__ = [
    "PeerHandler",
    "PeerRefreshHandler",
]


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
                    print(f"[{SERVICE_NAME} {datetime.now()}] Error when processing notify from peer {peer_url}.\n"
                          f"    Error: {str(e)}", flush=True)

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


# noinspection PyAbstractClass
class PeerRefreshHandler(RequestHandler):
    async def post(self):
        """
        Handles refreshing the peer list if a user decides to clear existing nodes.
        """

        # Clear all peers; re-insert self and registry nodes
        clear_db_and_insert_fixed_nodes()

        # Invalidate in-memory peer cache and force a refresh
        self.application.peer_manager.peer_cache_invalidated = True
        self.application.peer_manager.get_peers()

        self.clear()
        self.set_status(204)
