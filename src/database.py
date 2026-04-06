from sqlalchemy import create_engine, text
from config import DATABASE_URL
 
# Create the SQLAlchemy engine using the configured DATABASE_URL
# echo=False prevents SQLAlchemy from printing every SQL query to the console
engine = create_engine(DATABASE_URL, echo=False)
 
 
def init_database():
    """
    Initializes the database by creating the 'weather_readings' table if it doesn't exist.
    This is called on startup to ensure the database is ready to receive data.
    """
    # Define the schema for the weather_readings table
    # Using raw SQL here instead of ORM models for simplicity and direct control
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
        # Connect to the database and execute the table creation query
        with engine.connect() as conn:
            conn.execute(create_table_query)
            conn.commit() # Commit the transaction to save changes
        print("  [DB] Table 'weather_readings' is ready")
        return True
    except Exception as e:
        # Catch and log any database connection or execution errors
        print(f"  [DB ERROR] Failed to initialize database: {e}")
        return False
 
 
def get_row_count():
    """
    Helper function to get the total number of rows in the 'weather_readings' table.
    Used to track how many records were inserted during a pipeline run.
    """
    try:
        # Execute a simple COUNT query
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM weather_readings"))
            return result.scalar() # Return the single scalar value (the count)
    except Exception:
        # If the table doesn't exist yet or there's an error, return 0
        return 0
