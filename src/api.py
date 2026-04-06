from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from database import engine, init_database
from schemas import WeatherReading, CityStats, LatestReading, PipelineResponse
from extract import fetch_all_cities
from transform import process_data
from load import save_to_db
from config import CITIES
 
 
# Initialize the FastAPI application with basic metadata
app = FastAPI(
    title="Weather Data Pipeline API",
    description="REST API for weather data collected from OpenWeatherMap",
    version="1.0.0",
)
 
 
@app.on_event("startup")
def on_startup():
    """
    Hook that runs when the FastAPI application starts.
    Ensures the database tables are created before handling any requests.
    """
    init_database()
 
 
@app.get("/")
def root():
    """
    Root endpoint returning basic API information and a list of available endpoints.
    This serves as a simple landing page for the API.
    """
    return {
        "name": "Weather Data Pipeline API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "/weather",
            "/weather/{city}",
            "/weather/stats",
            "/weather/latest",
            "/weather/fetch",
        ]
    }
 
 
@app.get("/weather", response_model=list[WeatherReading])
def get_all_readings(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """
    Retrieves all weather readings from the database with pagination support.
    Results are ordered by recorded_at descending (newest first).
    """
    with engine.connect() as conn:
        # Execute a raw SQL query to fetch the data
        # Using parameterized queries to prevent SQL injection
        result = conn.execute(
            text("""
                SELECT id, city, temperature, feels_like, humidity,
                       pressure, wind_speed, description, clouds, recorded_at
                FROM weather_readings
                ORDER BY recorded_at DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset}
        )
        rows = result.fetchall()
        
    # Return an empty list if no data is found
    if not rows:
        return []
        
    # Convert the raw database rows into Pydantic models for the API response
    return [
        WeatherReading(
            id=row[0], city=row[1], temperature=row[2], feels_like=row[3],
            humidity=row[4], pressure=row[5], wind_speed=row[6],
            description=row[7], clouds=row[8], recorded_at=row[9]
        )
        for row in rows
    ]
 
 
@app.get("/weather/stats", response_model=list[CityStats])
def get_stats():
    """
    Calculates and returns aggregated statistics (average, max, min temperature, etc.) for each city.
    Note: This endpoint is defined BEFORE /weather/{city} to avoid route matching conflicts.
    """
    with engine.connect() as conn:
        # Execute a raw SQL query to calculate the statistics
        # Grouping by city and ordering by average temperature descending
        result = conn.execute(
            text("""
                SELECT
                    city,
                    ROUND(AVG(temperature)::numeric, 1) as avg_temperature,
                    ROUND(MIN(temperature)::numeric, 1) as min_temperature,
                    ROUND(MAX(temperature)::numeric, 1) as max_temperature,
                    ROUND(AVG(humidity)::numeric, 1) as avg_humidity,
                    COUNT(*) as total_readings
                FROM weather_readings
                GROUP BY city
                ORDER BY avg_temperature DESC
            """)
        )
        rows = result.fetchall()
        
    # Raise a 404 error if no data is available to calculate stats
    if not rows:
        raise HTTPException(status_code=404, detail="No data available")
        
    # Convert the raw database rows into Pydantic models for the API response
    return [
        CityStats(
            city=row[0], avg_temperature=float(row[1]),
            min_temperature=float(row[2]), max_temperature=float(row[3]),
            avg_humidity=float(row[4]), total_readings=row[5]
        )
        for row in rows
    ]
 
 
@app.get("/weather/latest", response_model=list[LatestReading])
def get_latest():
    """
    Retrieves the most recent weather reading for each city.
    Note: This endpoint is defined BEFORE /weather/{city} to avoid route matching conflicts.
    """
    with engine.connect() as conn:
        # Execute a raw SQL query to fetch the latest reading for each city
        # Using DISTINCT ON (city) to get the first row for each city after ordering by recorded_at descending
        result = conn.execute(
            text("""
                SELECT DISTINCT ON (city)
                    city, temperature, humidity, description, recorded_at
                FROM weather_readings
                ORDER BY city, recorded_at DESC
            """)
        )
        rows = result.fetchall()
        
    # Raise a 404 error if no data is available
    if not rows:
        raise HTTPException(status_code=404, detail="No data available")
        
    # Convert the raw database rows into Pydantic models for the API response
    return [
        LatestReading(
            city=row[0], temperature=row[1], humidity=row[2],
            description=row[3], recorded_at=row[4]
        )
        for row in rows
    ]
 
 
@app.get("/weather/{city}", response_model=list[WeatherReading])
def get_city_readings(
    city: str,
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Retrieves all weather readings for a specific city with pagination support.
    Results are ordered by recorded_at descending (newest first).
    """
    with engine.connect() as conn:
        # Execute a raw SQL query to fetch the data for the specified city
        # Using ILIKE for case-insensitive matching
        result = conn.execute(
            text("""
                SELECT id, city, temperature, feels_like, humidity,
                       pressure, wind_speed, description, clouds, recorded_at
                FROM weather_readings
                WHERE city ILIKE :city
                ORDER BY recorded_at DESC
                LIMIT :limit
            """),
            {"city": city, "limit": limit}
        )
        rows = result.fetchall()
        
    # Raise a 404 error if no readings are found for the specified city
    if not rows:
        raise HTTPException(status_code=404, detail=f"No readings found for city: {city}")
        
    # Convert the raw database rows into Pydantic models for the API response
    return [
        WeatherReading(
            id=row[0], city=row[1], temperature=row[2], feels_like=row[3],
            humidity=row[4], pressure=row[5], wind_speed=row[6],
            description=row[7], clouds=row[8], recorded_at=row[9]
        )
        for row in rows
    ]
 
 
@app.post("/weather/fetch", response_model=PipelineResponse)
def trigger_fetch():
    """
    Manually triggers the ETL pipeline to fetch, transform, and load new weather data.
    This endpoint allows updating the data on demand without waiting for the scheduler.
    """
    try:
        # Step 1: Extract data from the OpenWeatherMap API
        records = fetch_all_cities(CITIES)
        if not records:
            raise HTTPException(status_code=500, detail="Failed to fetch weather data")
            
        # Step 2: Transform the raw data into a pandas DataFrame
        df = process_data(records)
        
        # Step 3: Load the processed data into the PostgreSQL database
        save_to_db(df)
        
        # Return a success response with the number of rows inserted
        return PipelineResponse(
            status="success",
            message=f"Pipeline completed for {len(CITIES)} cities",
            rows_inserted=len(records)
        )
    except HTTPException:
        # Re-raise HTTP exceptions to return the correct status code and detail
        raise
    except Exception as e:
        # Catch any other unexpected errors and return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail=str(e))
