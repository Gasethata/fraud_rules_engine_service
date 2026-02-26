"""
Testing API Endpoints
    - Check if the login works
    - What happens when the credentials are wrong
    - Show that flagged transactions can be retrieved with a token
"""
import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.api

def test_login_success(client):
    """Test that valid credentials return token"""
    response = client.post("/login", json={
        "username": "admin", 
        "password": "admin"
    })
    assert response.status_code == 200
    assert "access_token" in response.json()

def test_login_failure(client):
    """Test that invalid credentials are rejected"""
    response = client.post("/login", json={
        "username": "admin", 
        "password": "wrong"
    })
    assert response.status_code == 401

def test_flagged_requires_auth(client):
    """Test that protected endpoint rejects unauthenticated requests"""
    response = client.get("/flagged")
    assert response.status_code == 401

def test_flagged_with_token(client, auth_token):
    """Test that authenticated users can access flagged transactions"""
    response = client.get(
        "/flagged",
        headers={"Authorization": f"Bearer {auth_token}"}
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)