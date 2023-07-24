#!/bin/bash

cd /aggregation || exit

# The default base image entrypoint takes care of creating bento_user and configuring git

# Update dependencies and install module locally
/poetry_user_install_dev.bash

export BENTO_DEBUG=true
export CHORD_DEBUG=true

# Set default internal port to 5000
: "${INTERNAL_PORT:=5000}"

# Set internal debug port, falling back to default in a Bento deployment
: "${DEBUGGER_PORT:=5684}"

python -m debugpy --listen "0.0.0.0:${DEBUGGER_PORT}" -m uvicorn \
  bento_aggregation_service.app:application \
  --host 0.0.0.0 \
  --port "${INTERNAL_PORT}" \
  --reload
