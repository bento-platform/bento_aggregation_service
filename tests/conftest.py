import pytest
import os
from fastapi.testclient import TestClient


os.environ["BENTO_DEBUG"] = "true"
os.environ["BENTO_AUTHZ_SERVICE_URL"] = "https://authz.local"
os.environ["KATSU_URL"] = "https://katsu.local"
os.environ["SERVICE_REGISTRY_URL"] = "https://sr.local"
os.environ["CORS_ORIGINS"] = "*"


@pytest.fixture
def test_client():
    from bento_aggregation_service.app import application

    with TestClient(application) as client:
        yield client
