#!/bin/bash

cd /aggregation || exit

# Set .gitconfig for development
/set_gitconfig.bash

source /env/bin/activate

# Update dependencies and install module locally (similar to pip install -e: "editable mode")
poetry install

export BENTO_DEBUG=true
export CHORD_DEBUG=true

python3 ./run.py
