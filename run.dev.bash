#!/bin/bash

cd /aggregation || exit

# The default base image entrypoint takes care of creating bento_user and configuring git

# Update dependencies and install module locally
/poetry_user_install_dev.bash

export BENTO_DEBUG=true
export CHORD_DEBUG=true

python3 ./run.py
