from sqlalchemy import create_engine, text
from config import DATABASE_URL
 
# echo=False keeps SQLAlchemy quiet — I don't need it printing every SELECT
# to the console on every API request. Turning it on temporarily is useful
# when debugging a query, but it's too noisy for normal operation.
engine = create_engine(DATABASE_URL, echo=False)
 
 
def init_database():
    """
    Creates the weather_readings table if it doesn't already exist.
    I call this on startup so the app never crashes on a fresh database —
    it just creates what it needs and moves on.
    """
    # I wrote this as raw SQL rather than an ORM model because the schema is simple
    # and I wanted full control over the column types and constraints.
    # SERIAL handles the auto-increment primary key, and DEFAULT NOW() means
    # I don't have to pass recorded_at explicitly on every insert.
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
    """
    Returns the current total number of rows in weather_readings.
    I use this in the load step to calculate exactly how many rows were inserted
    in a given pipeline run (rows_after - rows_before).
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM weather_readings"))
            return result.scalar()
    except Exception:
        # If the table doesn't exist yet, COUNT will fail — returning 0 is the safe fallback
        return 0
