#!/bin/bash

# Set default internal port to 5000
: "${INTERNAL_PORT:=5000}"

uvicorn bento_aggregation_service.app:application \
  --workers 1 \
  --loop uvloop \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}"
