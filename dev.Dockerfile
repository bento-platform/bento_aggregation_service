FROM ghcr.io/bento-platform/bento_base_image:python-debian-latest

WORKDIR /aggregation

COPY requirements.txt .
COPY requirements.dev.txt .
RUN pip install -r requirements.txt

# Don't copy code in, since it gets mounted in with development mode.

ENV CHORD_DEBUG=True
ENTRYPOINT ["python3", "run.py"]
