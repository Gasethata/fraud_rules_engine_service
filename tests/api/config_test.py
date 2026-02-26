import pytest
from fastapi.testclient import TestClient
from main import app

pytestmark = pytest.mark.api

@pytest.fixture
def client():
    """Provide test client for API requests"""
    return TestClient(app)

@pytest.fixture
def auth_token(client):
    """Get authentication token for tests"""
    response = client.post(
        "/login",
        json={"username": "admin", "password": "admin"}
    )
    return response.json()["access_token"]