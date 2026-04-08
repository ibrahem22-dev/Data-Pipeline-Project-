from pydantic import BaseModel, ConfigDict
from datetime import datetime


class WeatherReading(BaseModel):
    """
    Represents a single row from the weather_readings table.
    Most fields are optional because the OpenWeatherMap API occasionally
    omits them for certain cities or weather conditions.
    """
    id: int
    city: str
    temperature: float
    feels_like: float | None = None
    humidity: float
    pressure: float | None = None
    wind_speed: float | None = None
    description: str | None = None
    clouds: float | None = None
    recorded_at: datetime | None = None

    # from_attributes=True lets Pydantic read data from SQLAlchemy row objects
    # directly, without me having to manually convert them to dicts first.
    # This replaces the old `class Config` syntax from Pydantic v1.
    model_config = ConfigDict(from_attributes=True)


class CityStats(BaseModel):
    """
    Aggregated statistics for a single city — returned by the /weather/stats endpoint.
    These are all computed in SQL (AVG, MIN, MAX, COUNT) rather than in Python
    so the database does the heavy lifting.
    """
    city: str
    avg_temperature: float
    min_temperature: float
    max_temperature: float
    avg_humidity: float
    total_readings: int


class LatestReading(BaseModel):
    """
    The most recent weather snapshot for a city — returned by /weather/latest.
    I kept this model lighter than WeatherReading since the latest endpoint
    only needs the fields that are relevant for a "current conditions" view.
    """
    city: str
    temperature: float
    humidity: float
    description: str | None = None
    recorded_at: datetime | None = None


class PipelineResponse(BaseModel):
    """
    Response model for the /weather/fetch endpoint.
    Gives the caller a clear confirmation of what happened:
    whether it succeeded, a human-readable message, and how many rows were inserted.
    """
    status: str
    message: str
    rows_inserted: int
