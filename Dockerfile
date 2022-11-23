FROM ghcr.io/bento-platform/bento_base_image:python-debian-latest

WORKDIR /aggregation

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY run.py .
COPY bento_aggregation_service bento_aggregation_service

ENTRYPOINT ["python3", "run.py"]
