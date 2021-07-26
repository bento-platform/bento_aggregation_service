# Bento Federation Service

A service for federated search between Bento platform nodes.

## Environment Variables

`DATABASE`: Defaults to `data/federation.db`

Keeps track of a list of peers to pass federated queries to.

`CHORD_DEBUG`: `true` (insecure) or `false`; default is `false`

`BENTO_FEDERATION_MODE`: `true` or `false`; default is `true`

If set to false, the peer-contacting process will be skipped entirely.

`CHORD_URL`: ex. `http://127.0.0.1:5000/`

By convention, this *should* have a trailing slash; however as of v0.9.1 this 
is optional.

`CHORD_REGISTRY_URL`: ex. `http://127.0.0.1:5000/`

By convention, this *should* have a trailing slash; however as of v0.9.1 this 
is optional.

`OIDC_DISCOVERY_URI`:
ex. `https://keycloak.example.og/auth/realms/master/.well-known/openid-configuration`

By convention *should not* have a trailing slash.

`PORT`: Specified when running via `./run.py`; defaults to `5000`

`SERVICE_URL_BASE_PATH`: Base URL fragment (e.g. `/test/`) for endpoints

`SOCKET`: Specifies Unix socket location for production deployment
