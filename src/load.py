import pandas as pd
import os
from config import OUTPUT_FILE
from database import engine, get_row_count
 
 
def save_to_db(df: pd.DataFrame) -> bool:
    """
    Saves the processed pandas DataFrame to the PostgreSQL database.
    Uses SQLAlchemy's to_sql method for efficient bulk insertion.
    """
    # Don't try to save an empty DataFrame
    if df.empty:
        print("  [WARNING] No data to save — DataFrame is empty")
        return False
 
    try:
        # Select only the columns that match our database schema
        # This prevents errors if we added extra columns during transformation (like temp_diff)
        db_columns = [
            "city", "temperature", "feels_like", "humidity",
            "pressure", "wind_speed", "description", "clouds", "recorded_at"
        ]
        df_to_save = df[db_columns].copy()
 
        # Track row count before insertion to verify success
        rows_before = get_row_count()
        
        # Append the data to the 'weather_readings' table
        # index=False prevents pandas from saving the row index as a column
        df_to_save.to_sql("weather_readings", engine, if_exists="append", index=False)
        
        # Track row count after insertion
        rows_after = get_row_count()
 
        # Calculate and print the number of inserted rows
        inserted = rows_after - rows_before
        print(f"  [DB] Inserted {inserted} rows → weather_readings (total: {rows_after})")
        return True
 
    except Exception as e:
        # Catch and log any database insertion errors
        print(f"  [DB ERROR] Failed to save: {e}")
        return False
 
 
def save_to_csv(df: pd.DataFrame, filename: str = OUTPUT_FILE) -> bool:
    """
    Saves the processed pandas DataFrame to a CSV file as a backup.
    This dual-storage approach (DB + CSV) ensures we don't lose data if the DB goes down.
    """
    # Don't try to save an empty DataFrame
    if df.empty:
        print("  [WARNING] No data to save — DataFrame is empty")
        return False
 
    try:
        # Check if the file exists to determine whether to write the header row
        file_exists = os.path.exists(filename)
        
        # Append the data to the CSV file
        # mode="a" means append, so we don't overwrite existing data
        df.to_csv(
            filename,
            mode="a",
            header=not file_exists, # Only write header if the file is new
            index=False,
            encoding="utf-8",
        )
        print(f"  [CSV] {len(df)} records → {filename}")
        return True
 
    except PermissionError:
        # Handle the common case where the CSV file is open in Excel or another program
        print(f"  [ERROR] Cannot write to {filename} — file is open in another program")
        return False
 
    except Exception as e:
        # Catch and log any other file writing errors
        print(f"  [ERROR] Failed to save: {e}")
        return False
