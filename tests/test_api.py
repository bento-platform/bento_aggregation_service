from fastapi.testclient import TestClient


def test_service_info(test_client: TestClient):
    response = test_client.get("/service-info")
    assert response.status_code == 200
