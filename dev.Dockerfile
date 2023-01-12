FROM ghcr.io/bento-platform/bento_base_image:python-debian-latest

RUN pip install --no-cache-dir poetry==1.3.2 "uvicorn[standard]==0.20.0"

WORKDIR /aggregation

COPY pyproject.toml pyproject.toml
COPY poetry.toml poetry.toml
COPY poetry.lock poetry.lock

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry install --no-root

# Don't copy code in, since it gets mounted in with development mode.

ENV CHORD_DEBUG=True
ENTRYPOINT ["python3", "run.py"]
