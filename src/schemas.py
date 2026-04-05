from pydantic import BaseModel
from datetime import datetime
 
 
class WeatherReading(BaseModel):

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
        from_attributes = True
 
 
class CityStats(BaseModel):

    city: str
    avg_temperature: float
    min_temperature: float
    max_temperature: float
    avg_humidity: float
    total_readings: int
 
 
class LatestReading(BaseModel):

    city: str
    temperature: float
    humidity: float
    description: str | None = None
    recorded_at: datetime | None = None
 
 
class PipelineResponse(BaseModel):

    status: str
    message: str
    rows_inserted: int
 