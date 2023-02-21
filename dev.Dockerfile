FROM ghcr.io/bento-platform/bento_base_image:python-debian-2023.02.21

RUN pip install --no-cache-dir poetry==1.3.2 "uvicorn[standard]==0.20.0"

WORKDIR /aggregation

COPY pyproject.toml .
COPY poetry.toml .
COPY poetry.lock .

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry install --no-root

# Don't copy code in, since it gets mounted in with development mode.
# Copy in an entrypoint + runner script so we have somewhere to start.

COPY run.dev.bash .

ENV CHORD_DEBUG=True

# Use base image entrypoint for dropping down into bento_user & running this CMD
CMD ["bash", "./run.dev.bash"]
