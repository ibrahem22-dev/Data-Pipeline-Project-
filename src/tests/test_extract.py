import pytest
from unittest.mock import patch, MagicMock
from extract import fetch_weather
 
 
def _mock_api_response():

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "main": {
            "temp": 25.0,
            "feels_like": 24.0,
            "humidity": 50,
            "pressure": 1013,
        },
        "wind": {"speed": 3.5},
        "weather": [{"description": "clear sky"}],
        "clouds": {"all": 10},
    }
    mock_response.raise_for_status = MagicMock()
    return mock_response
 
 
@patch("extract.requests.get")
def test_fetch_weather_success(mock_get):

    mock_get.return_value = _mock_api_response()
    result = fetch_weather("Nazareth,IL")
    assert result is not None
    assert result["city"] == "Nazareth"
    assert result["temperature"] == 25.0
    assert result["humidity"] == 50
    assert "recorded_at" in result
 
 
@patch("extract.requests.get")
def test_fetch_weather_has_all_fields(mock_get):

    mock_get.return_value = _mock_api_response()
    result = fetch_weather("Nazareth,IL")
    expected_fields = [
        "city", "temperature", "feels_like", "humidity",
        "pressure", "wind_speed", "description", "clouds", "recorded_at"
    ]
    for field in expected_fields:
        assert field in result
 
 
@patch("extract.requests.get")
def test_fetch_weather_timeout(mock_get):

    import requests
    mock_get.side_effect = requests.exceptions.Timeout()
    result = fetch_weather("Nazareth,IL")
    assert result is None
 
 
@patch("extract.requests.get")
def test_fetch_weather_connection_error(mock_get):

    import requests
    mock_get.side_effect = requests.exceptions.ConnectionError()
    result = fetch_weather("Nazareth,IL")
    assert result is None
 
 
@patch("extract.requests.get")
def test_fetch_weather_http_error(mock_get):

    import requests
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.reason = "Not Found"
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=mock_response
    )
    mock_get.return_value = mock_response
    result = fetch_weather("FakeCity,XX")
    assert result is None
 
 
@patch("extract.requests.get")
def test_fetch_weather_city_name_extraction(mock_get):

    mock_get.return_value = _mock_api_response()
    result = fetch_weather("Tel Aviv,IL")
    assert result["city"] == "Tel Aviv"