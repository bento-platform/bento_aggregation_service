#!/bin/bash

# Set .gitconfig for development
/set_gitconfig.bash

export BENTO_DEBUG=true
export CHORD_DEBUG=true

python3 ./run.py
