import csv
import pandas as pd
import os
from config import OUTPUT_FILE


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
 
        print(f"  [SAVED] {len(df)} records → {filename}")
        return True
 
    except PermissionError:

        print(f"  [ERROR] Cannot write to {filename} — file is open in another program")
        return False
 
    except Exception as e:
        print(f"  [ERROR] Failed to save: {e}")
        return False