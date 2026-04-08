import pandas as pd
import os
from config import OUTPUT_FILE
from database import engine, get_row_count
 
 
def save_to_db(df: pd.DataFrame) -> bool:
    """
    Saves the processed DataFrame to the PostgreSQL database.
    I use pandas' to_sql with if_exists="append" — I never want to overwrite
    historical readings, only add new ones on top.
    """
    if df.empty:
        print("  [WARNING] No data to save — DataFrame is empty")
        return False
 
    try:
        # I only pass the columns that actually exist in the DB schema.
        # The transform step adds temp_diff and temp_category which are useful
        # for analysis but I didn't include them in the table — keeping the schema lean.
        db_columns = [
            "city", "temperature", "feels_like", "humidity",
            "pressure", "wind_speed", "description", "clouds", "recorded_at"
        ]
        df_to_save = df[db_columns].copy()
 
        # Snapshot the row count before and after so I can confirm how many rows actually landed
        rows_before = get_row_count()
        
        # index=False because I don't want pandas' 0-based row index saved as a column —
        # the DB has its own SERIAL primary key for that
        df_to_save.to_sql("weather_readings", engine, if_exists="append", index=False)
        
        rows_after = get_row_count()
 
        inserted = rows_after - rows_before
        print(f"  [DB] Inserted {inserted} rows → weather_readings (total: {rows_after})")
        return True
 
    except Exception as e:
        print(f"  [DB ERROR] Failed to save: {e}")
        return False
 
 
def save_to_csv(df: pd.DataFrame, filename: str = OUTPUT_FILE) -> bool:
    """
    Saves the processed DataFrame to a CSV file as a local backup.
    I keep both DB and CSV outputs running in parallel — if the database
    goes down or I need to inspect the raw data quickly, the CSV is there.
    """
    if df.empty:
        print("  [WARNING] No data to save — DataFrame is empty")
        return False
 
    try:
        # Check whether the file already exists so I know whether to write the header.
        # If I always write the header, I end up with duplicate header rows mid-file.
        file_exists = os.path.exists(filename)
        
        # mode="a" appends to the file instead of overwriting it —
        # I want the full history in one CSV, not just the latest batch
        df.to_csv(
            filename,
            mode="a",
            header=not file_exists,
            index=False,
            encoding="utf-8",
        )
        print(f"  [CSV] {len(df)} records → {filename}")
        return True
 
    except PermissionError:
        # This happens when the CSV is open in Excel — common enough that it deserves its own message
        print(f"  [ERROR] Cannot write to {filename} — file is open in another program")
        return False
 
    except Exception as e:
        print(f"  [ERROR] Failed to save: {e}")
        return False
