import pandas as pd
import os
from config import OUTPUT_FILE
from database import engine, get_row_count
 
 
def save_to_db(df: pd.DataFrame) -> bool:
 
    if df.empty:
        print("  [WARNING] No data to save — DataFrame is empty")
        return False
 
    try:
      
        db_columns = [
            "city", "temperature", "feels_like", "humidity",
            "pressure", "wind_speed", "description", "clouds", "recorded_at"
        ]
        df_to_save = df[db_columns].copy()
 
        rows_before = get_row_count()
        df_to_save.to_sql("weather_readings", engine, if_exists="append", index=False)
        rows_after = get_row_count()
 
        inserted = rows_after - rows_before
        print(f"  [DB] Inserted {inserted} rows → weather_readings (total: {rows_after})")
        return True
 
    except Exception as e:
        print(f"  [DB ERROR] Failed to save: {e}")
        return False
 
 
def save_to_csv(df: pd.DataFrame, filename: str = OUTPUT_FILE) -> bool:
    if df.empty:
        print("  [WARNING] No data to save — DataFrame is empty")
        return False
 
    try:
        file_exists = os.path.exists(filename)
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
        print(f"  [ERROR] Cannot write to {filename} — file is open in another program")
        return False
 
    except Exception as e:
        print(f"  [ERROR] Failed to save: {e}")
        return False