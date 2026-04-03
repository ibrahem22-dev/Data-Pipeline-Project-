from sqlalchemy import create_engine, text
from config import DATABASE_URL
 

engine = create_engine(DATABASE_URL, echo=False)
 
 
def init_database():
    create_table_query = text("""
        CREATE TABLE IF NOT EXISTS weather_readings (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            temperature FLOAT NOT NULL,
            feels_like FLOAT,
            humidity FLOAT NOT NULL,
            pressure FLOAT,
            wind_speed FLOAT,
            description TEXT,
            clouds FLOAT,
            recorded_at TIMESTAMP DEFAULT NOW()
        );
    """)
 
    try:
        with engine.connect() as conn:
            conn.execute(create_table_query)
            conn.commit()
        print("  [DB] Table 'weather_readings' is ready")
        return True
    except Exception as e:
        print(f"  [DB ERROR] Failed to initialize database: {e}")
        return False
 
 
def get_row_count():

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM weather_readings"))
            return result.scalar()
    except Exception:
        return 0
 