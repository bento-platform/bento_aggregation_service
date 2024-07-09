FROM ghcr.io/bento-platform/bento_base_image:python-debian-2024.07.09

LABEL org.opencontainers.image.description="Local development image the Bento aggregation service."
LABEL devcontainer.metadata='[{ \
  "remoteUser": "bento_user", \
  "customizations": { \
    "vscode": { \
      "extensions": ["ms-python.python", "eamodio.gitlens"], \
      "settings": {"workspaceFolder": "/aggregation"} \
    } \
  } \
}]'

# Run as root in the Dockerfile until we drop down to the service user in the entrypoint
USER root

RUN pip install --no-cache-dir "uvicorn[standard]==0.30.1"

WORKDIR /aggregation

COPY pyproject.toml .
COPY poetry.lock .

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry config virtualenvs.create false && \
    poetry install --no-root

# Don't copy code in, since it gets mounted in with development mode.
# Copy in an entrypoint + runner script so we have somewhere to start.

COPY run.dev.bash .

ENV BENTO_DEBUG=True

# Use base image entrypoint for dropping down into bento_user & running this CMD
CMD ["bash", "./run.dev.bash"]
