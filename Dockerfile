FROM ghcr.io/bento-platform/bento_base_image:python-debian-2023.08.16

# Run as root in the Dockerfile until we drop down to the service user in the entrypoint
USER root

# Use uvicorn (instead of hypercorn) in production since I've found
# multiple benchmarks showing it to be faster - David L
RUN pip install --no-cache-dir "uvicorn[standard]==0.23.2"

WORKDIR /aggregation

COPY pyproject.toml .
COPY poetry.lock .

# Install production dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry config virtualenvs.create false && \
    poetry install --without dev --no-root

# Manually copy only what's relevant
# (Don't use .dockerignore, which allows us to have development containers too)
COPY bento_aggregation_service bento_aggregation_service
COPY LICENSE .
COPY README.md .
COPY run.bash .

# Install the module itself, locally (similar to `pip install -e .`)
RUN poetry install --without dev

# Use base image entrypoint for dropping down into bento_user & running this CMD
CMD ["bash", "./run.bash"]
