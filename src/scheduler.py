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
    Executes the complete ETL pipeline: Extract -> Transform -> Load.
    This function is called by the scheduler at regular intervals.
    """
    # Print a clear header to distinguish scheduled runs in the console logs
    print(f"\n{'#'*60}")
    print(f"  SCHEDULED RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
 
    # Step 1: Extract data from the OpenWeatherMap API
    records = fetch_all_cities(CITIES)
 
    # Stop the pipeline if extraction failed to avoid processing empty data
    if not records:
        print("  [ERROR] No data fetched — skipping this run")
        return
 
    # Step 2: Transform the raw data into a pandas DataFrame
    df = process_data(records)
    
    # Step 3: Load the processed data into both PostgreSQL and CSV
    save_to_db(df)
    save_to_csv(df)
    
    # Print a summary of the processed data for easy monitoring
    print_summary(df)
 
    # Indicate that the run is complete and the scheduler is waiting for the next interval
    print(f"  [DONE] Next run scheduled automatically\n")
 
 
def start_scheduler(interval_hours: int = 6):
    """
    Initializes the database, runs the pipeline once immediately,
    and then sets up the scheduler to run it periodically.
    """
    # Check if the API key is configured before starting the scheduler
    if API_KEY == "":
        print("\n  [ERROR] API key not configured!")
        return
 
    # Ensure the database tables exist before trying to save data
    if not init_database():
        print("  [ERROR] Database initialization failed")
        return
 
    # Print a startup message indicating the configured interval
    print(f"  [SCHEDULER] Starting with {interval_hours}-hour interval")
    
    # Run the pipeline immediately so we don't have to wait for the first interval
    run_pipeline()
 
    # Schedule the pipeline to run every 'interval_hours' hours
    schedule.every(interval_hours).hours.do(run_pipeline)
 
    # Print instructions on how to stop the scheduler
    print(f"  [SCHEDULER] Running every {interval_hours} hours")
    print(f"  [SCHEDULER] Press Ctrl+C to stop\n")
 
    try:
        # Keep the script running indefinitely, checking for pending scheduled tasks every minute
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully to exit the scheduler loop
        print(f"\n  [SCHEDULER] Stopped by user")
 
 
if __name__ == "__main__":
    # If this script is run directly, start the scheduler with the default 6-hour interval
    start_scheduler(interval_hours=6)
