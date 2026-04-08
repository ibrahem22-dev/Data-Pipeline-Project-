from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from database import engine, init_database
from schemas import WeatherReading, CityStats, LatestReading, PipelineResponse
from extract import fetch_all_cities
from transform import process_data
from load import save_to_db
from config import CITIES


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs setup logic before the app starts accepting requests.
    I use the lifespan pattern here instead of the old @app.on_event("startup")
    which was deprecated in FastAPI 0.93 — this is the current recommended approach.
    """
    init_database()
    yield  # Everything before yield = startup, everything after = shutdown


app = FastAPI(
    title="Weather Data Pipeline API",
    description="REST API for weather data collected from OpenWeatherMap",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
def root():
    """
    Simple landing page for the API — useful when someone hits the root URL
    and wants to know what endpoints are available without opening /docs.
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
    Returns all weather readings with pagination.
    I cap the limit at 500 to avoid accidentally returning the entire table
    in a single response — the table will grow indefinitely as the scheduler runs.
    """
    with engine.connect() as conn:
        # Parameterized query — I'm not building SQL strings by hand,
        # that's how you end up with injection vulnerabilities
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
        
    if not rows:
        return []
        
    # Map raw DB rows to Pydantic models by position — the SELECT order matches
    # the field order in WeatherReading, so this is safe and readable
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
    Returns aggregated stats (avg/min/max temperature, avg humidity, total readings) per city.
    This endpoint is defined before /weather/{city} to prevent FastAPI from
    treating "stats" as a city name — route order matters in FastAPI.
    """
    with engine.connect() as conn:
        # ::numeric cast is needed for ROUND() in PostgreSQL — it doesn't accept FLOAT directly
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
        
    if not rows:
        raise HTTPException(status_code=404, detail="No data available")
        
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
    Returns the most recent reading for each city.
    Also defined before /weather/{city} for the same route-ordering reason as /stats.
    """
    with engine.connect() as conn:
        # DISTINCT ON is a PostgreSQL-specific feature — it keeps only the first row
        # per city after sorting by recorded_at DESC, which gives me the latest reading.
        # Standard SQL would need a subquery or window function to do the same thing.
        result = conn.execute(
            text("""
                SELECT DISTINCT ON (city)
                    city, temperature, humidity, description, recorded_at
                FROM weather_readings
                ORDER BY city, recorded_at DESC
            """)
        )
        rows = result.fetchall()
        
    if not rows:
        raise HTTPException(status_code=404, detail="No data available")
        
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
    Returns readings for a specific city.
    ILIKE makes the match case-insensitive so "tel aviv", "Tel Aviv", and "TEL AVIV"
    all return the same results — I don't want users to have to know the exact casing.
    """
    with engine.connect() as conn:
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
        
    if not rows:
        raise HTTPException(status_code=404, detail=f"No readings found for city: {city}")
        
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
    Manually triggers a full ETL run on demand.
    Useful when I want fresh data immediately without waiting for the scheduler —
    for example, right after deploying or when demoing the project.
    """
    try:
        records = fetch_all_cities(CITIES)
        if not records:
            raise HTTPException(status_code=500, detail="Failed to fetch weather data")
            
        df = process_data(records)
        save_to_db(df)
        
        return PipelineResponse(
            status="success",
            message=f"Pipeline completed for {len(CITIES)} cities",
            rows_inserted=len(records)
        )
    except HTTPException:
        # Re-raise FastAPI's own exceptions unchanged — they already have the right status code
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
