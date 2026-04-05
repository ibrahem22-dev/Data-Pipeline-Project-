from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from database import engine, init_database
from schemas import WeatherReading, CityStats, LatestReading, PipelineResponse
from extract import fetch_all_cities
from transform import process_data
from load import save_to_db
from config import CITIES


app = FastAPI(
    title="Weather Data Pipeline API",
    description="REST API for weather data collected from OpenWeatherMap",
    version="1.0.0",
)


@app.on_event("startup")
def on_startup():
    init_database()


@app.get("/")
def root():
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
    with engine.connect() as conn:
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
    with engine.connect() as conn:
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
    with engine.connect() as conn:
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
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))