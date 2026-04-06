from pydantic import BaseModel
from datetime import datetime
 
 
class WeatherReading(BaseModel):
    """
    Pydantic model representing a single weather reading.
    This defines the structure of the data returned by the API for individual readings.
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
 
    class Config:
        # Enable ORM mode to allow Pydantic to read data from SQLAlchemy models
        # This is crucial for converting database rows into API responses
        from_attributes = True
 
 
class CityStats(BaseModel):
    """
    Pydantic model representing aggregated statistics for a specific city.
    Used by the /weather/stats endpoint to return calculated metrics.
    """
    city: str
    avg_temperature: float
    min_temperature: float
    max_temperature: float
    avg_humidity: float
    total_readings: int
 
 
class LatestReading(BaseModel):
    """
    Pydantic model representing the most recent weather reading for a city.
    Used by the /weather/latest endpoint to return the current weather state.
    """
    city: str
    temperature: float
    humidity: float
    description: str | None = None
    recorded_at: datetime | None = None
 
 
class PipelineResponse(BaseModel):
    """
    Pydantic model representing the response from a manual pipeline trigger.
    Used by the /weather/fetch endpoint to report the status of the ETL process.
    """
    status: str
    message: str
    rows_inserted: int
