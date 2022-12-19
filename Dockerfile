FROM ghcr.io/bento-platform/bento_base_image:python-debian-latest

# Use uvicorn (instead of hypercorn) in production since I've found
# multiple benchmarks showing it to be faster - David L
RUN pip install --no-cache-dir poetry==1.2.2 "uvicorn[standard]==0.20.0"

WORKDIR /aggregation

COPY pyproject.toml pyproject.toml
COPY poetry.toml poetry.toml
COPY poetry.lock poetry.lock

# Install production dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry install --without dev --no-root

# Manually copy only what's relevant
# (Don't use .dockerignore, which allows us to have development containers too)
COPY bento_aggregation_service bento_aggregation_service
COPY LICENSE LICENSE
COPY README.md README.md
COPY run.py run.py

# Install the module itself, locally (similar to `pip install -e .`)
RUN poetry install --without dev

ENTRYPOINT ["python3", "run.py"]
