# CHORD Federation

A service for federated search between CHORD nodes.

## Environment Variables

`DATABASE`: Defaults to `data/federation.db`

`CHORD_URL`: ex. `http://127.0.0.1:5000/`

`CHORD_HOST`: ex. `127.0.0.1:5000`

`CHORD_REGISTRY_URL`: ex. `http://127.0.0.1:5000/`

`PORT`: Specified when running via `./run.py`; defaults to `5000`

`SERVICE_URL_BASE_PATH`: Base URL fragment (e.g. `/test/`) for endpoints

`SOCKET`: Specifies Unix socket location for production deployment
