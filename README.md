# Bento Aggregation Service

A service for aggregating search results across Bento data services.

## Environment Variables

`CHORD_DEBUG`: `true` (insecure) or `false`; default is `false`

`PORT`: Specified when running via `./run.py`; defaults to `5000`

`SERVICE_URL_BASE_PATH`: Base URL fragment (e.g. `/test/`) for endpoints

Should usually be blank; set to non-blank to locally emulate a proxy prefix
like `/api/aggregation`.

`SOCKET`: Specifies Unix socket location for production deployment
