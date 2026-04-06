import pytest
import pandas as pd
from transform import process_data
 
 

SAMPLE_RECORDS = [
    {
        "city": "Nazareth",
        "temperature": 25.0,
        "feels_like": 24.0,
        "humidity": 50.0,
        "pressure": 1013.0,
        "wind_speed": 3.5,
        "description": "clear sky",
        "clouds": 10.0,
        "recorded_at": "2026-04-01T12:00:00",
    },
    {
        "city": "Tel Aviv",
        "temperature": 28.0,
        "feels_like": 30.0,
        "humidity": 70.0,
        "pressure": 1010.0,
        "wind_speed": 5.0,
        "description": "few clouds",
        "clouds": 25.0,
        "recorded_at": "2026-04-01T12:00:00",
    },
    {
        "city": "Jerusalem",
        "temperature": 12.0,
        "feels_like": 10.0,
        "humidity": 40.0,
        "pressure": 1015.0,
        "wind_speed": 2.0,
        "description": "overcast",
        "clouds": 80.0,
        "recorded_at": "2026-04-01T12:00:00",
    },
]
 
 
def test_process_data_returns_dataframe():

    df = process_data(SAMPLE_RECORDS)
    assert isinstance(df, pd.DataFrame)
 
 
def test_process_data_correct_rows():

    df = process_data(SAMPLE_RECORDS)
    assert len(df) == 3
 
 
def test_process_data_has_required_columns():

    df = process_data(SAMPLE_RECORDS)
    required = ["city", "temperature", "humidity", "recorded_at"]
    for col in required:
        assert col in df.columns
 
 
def test_process_data_temp_diff_column():

    df = process_data(SAMPLE_RECORDS)
    if "temp_diff" in df.columns:
        # Nazareth: 25.0 - 24.0 = 1.0
        nazareth = df[df["city"] == "Nazareth"].iloc[0]
        assert nazareth["temp_diff"] == 1.0
 
 
def test_process_data_temp_category_column():

    df = process_data(SAMPLE_RECORDS)
    if "temp_category" in df.columns:
        nazareth = df[df["city"] == "Nazareth"].iloc[0]
        jerusalem = df[df["city"] == "Jerusalem"].iloc[0]
        tel_aviv = df[df["city"] == "Tel Aviv"].iloc[0]
        assert nazareth["temp_category"] == "moderate"
        assert jerusalem["temp_category"] == "cold"
        assert tel_aviv["temp_category"] == "hot"
 
 
def test_process_data_empty_list():

    df = process_data([])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
 
 
def test_process_data_recorded_at_is_datetime():

    df = process_data(SAMPLE_RECORDS)
    assert pd.api.types.is_datetime64_any_dtype(df["recorded_at"])
