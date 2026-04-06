import pytest
from fastapi.testclient import TestClient
from api import app
 
 

client = TestClient(app)
 
 

def test_root():
    
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Weather Data Pipeline API"
    assert data["version"] == "1.0.0"
    assert "endpoints" in data
 
 

def test_get_all_readings():
   
    response = client.get("/weather")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
 
 
def test_get_all_readings_with_limit():
    
    response = client.get("/weather?limit=2")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 2
 
 
def test_get_all_readings_invalid_limit():
    
    response = client.get("/weather?limit=-1")
    assert response.status_code == 422
 
 
def test_get_all_readings_limit_too_high():
   
    response = client.get("/weather?limit=1000")
    assert response.status_code == 422
 
 

def test_get_stats():

    response = client.get("/weather/stats")
  
    assert response.status_code in [200, 404]
    if response.status_code == 200:
        data = response.json()
        assert isinstance(data, list)
        if len(data) > 0:
           
            assert "city" in data[0]
            assert "avg_temperature" in data[0]
            assert "avg_humidity" in data[0]
            assert "total_readings" in data[0]
 
 

def test_get_latest():

    response = client.get("/weather/latest")
    assert response.status_code in [200, 404]
    if response.status_code == 200:
        data = response.json()
        assert isinstance(data, list)
        if len(data) > 0:
            assert "city" in data[0]
            assert "temperature" in data[0]
 
 

def test_get_city_readings_valid():

    response = client.get("/weather/Nazareth")

    assert response.status_code in [200, 404]
    if response.status_code == 200:
        data = response.json()
        assert isinstance(data, list)

        for reading in data:
            assert reading["city"].lower() == "nazareth"
 
 
def test_get_city_readings_not_found():

    response = client.get("/weather/FakeCity12345")
    assert response.status_code == 404
    assert "No readings found" in response.json()["detail"]
 
 
def test_get_city_readings_case_insensitive():

    response_upper = client.get("/weather/NAZARETH")
    response_lower = client.get("/weather/nazareth")

    assert response_upper.status_code == response_lower.status_code
 
 

def test_trigger_fetch():

    response = client.post("/weather/fetch")

    assert response.status_code in [200, 500]
    if response.status_code == 200:
        data = response.json()
        assert data["status"] == "success"
        assert data["rows_inserted"] > 0
 
 

def test_nonexistent_endpoint():
    response = client.get("/nonexistent")
    assert response.status_code == 404
 