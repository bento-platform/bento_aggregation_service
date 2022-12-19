# Bento Aggregation Service

A service for aggregating search results across Bento data services.

## Environment Variables

`CHORD_DEBUG`: `true` (insecure) or `false`; default is `false`

`CHORD_URL`: ex. `http://127.0.0.1:5000/`

By convention, this *should* have a trailing slash; however as of v0.9.1 this 
is optional.

`PORT`: Specified when running via `./run.py`; defaults to `5000`

`SERVICE_URL_BASE_PATH`: Base URL fragment (e.g. `/test/`) for endpoints

Should usually be blank; set to non-blank to locally emulate a proxy prefix
like `/api/aggregation`.
