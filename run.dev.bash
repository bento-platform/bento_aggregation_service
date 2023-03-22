#!/bin/bash

cd /aggregation || exit

# Set .gitconfig for development
/set_gitconfig.bash

# Update dependencies and install module locally
/poetry_user_install_dev.bash

export BENTO_DEBUG=true
export CHORD_DEBUG=true

python3 ./run.py
