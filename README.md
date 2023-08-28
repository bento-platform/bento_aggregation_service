# Bento Aggregation Service

A service for aggregating search results across Bento data services.

## Environment Variables
The following environment variables are required for the Aggregation Service:

- `BENTO_DEBUG`: `true` (insecure) or `false`; default is `false`

- `USE_GOHAN`: `true` or `false` to use Gohan; default is `true`

- `KATSU_URL`: katsu service url (e.g. https://portal.bentov2.local/api/metadata/)

- `SERVICE_REGISTRY_URL`: service registry url (e.g. https://bentov2.local/api/service-registry/)

- `BENTO_AUTHZ_SERVICE_URL`: authorization service url (e.g. https://bentov2.local/api/authorization/)

By convention, URLs *should* have a trailing slash; however as of v0.9.1 this 
is optional.

Note that when deployed in a [Bento](https://github.com/bento-platform/bento) node, these environment variables are provided by the Aggregation docker-compose [file](https://github.com/bento-platform/bento/blob/main/lib/aggregation/docker-compose.aggregation.yaml).
