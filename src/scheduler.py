import schedule
import time
from datetime import datetime
from extract import fetch_all_cities
from load import save_to_db, save_to_csv
from transform import process_data, print_summary
from database import init_database
from config import API_KEY, CITIES
 
 
def run_pipeline():
    """
    Executes one full ETL cycle: Extract → Transform → Load.
    This is the function the scheduler calls on every tick.
    """
    # Clear visual separator so I can easily tell scheduled runs apart in the logs
    print(f"\n{'#'*60}")
    print(f"  SCHEDULED RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
 
    # Step 1: Extract
    records = fetch_all_cities(CITIES)
 
    # If extraction fails mid-schedule, I skip this run and wait for the next one
    # rather than crashing the whole scheduler process
    if not records:
        print("  [ERROR] No data fetched — skipping this run")
        return
 
    # Step 2: Transform
    df = process_data(records)
    
    # Step 3: Load
    save_to_db(df)
    save_to_csv(df)
    
    print_summary(df)
 
    print(f"  [DONE] Next run scheduled automatically\n")
 
 
def start_scheduler(interval_hours: int = 6):
    """
    Initializes the database, runs the pipeline immediately (so I don't have
    to wait a full interval for the first data point), then schedules it
    to repeat at the configured interval.
    """
    if API_KEY == "":
        print("\n  [ERROR] API key not configured!")
        return
 
    if not init_database():
        print("  [ERROR] Database initialization failed")
        return
 
    print(f"  [SCHEDULER] Starting with {interval_hours}-hour interval")
    
    # Run immediately on startup — I don't want to wait 6 hours for the first reading
    run_pipeline()
 
    schedule.every(interval_hours).hours.do(run_pipeline)
 
    print(f"  [SCHEDULER] Running every {interval_hours} hours")
    print(f"  [SCHEDULER] Press Ctrl+C to stop\n")
 
    try:
        # Poll every 60 seconds to check if a scheduled job is due.
        # This is lightweight enough that it doesn't noticeably consume resources.
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print(f"\n  [SCHEDULER] Stopped by user")
 
 
if __name__ == "__main__":
    # Running this file directly starts the scheduler with the default 6-hour interval
    start_scheduler(interval_hours=6)
