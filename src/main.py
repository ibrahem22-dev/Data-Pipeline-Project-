import sys
from extract import fetch_all_cities
from load import save_to_db, save_to_csv
from transform import process_data, print_summary
from database import init_database
from config import API_KEY, CITIES
 
 
def main():
    """
    Runs the full ETL pipeline once: Extract → Transform → Load.
    This is the function I call when I just want a single fresh batch of data
    without setting up the scheduler.
    """
    # Fail fast if the API key isn't set — no point hitting the network otherwise
    if API_KEY == "":
        print("\n  [ERROR] API key not configured!")
        print("  Steps to fix:")
        print("  1. Go to https://openweathermap.org/api and sign up (free)")
        print("  2. Copy your API key from the dashboard")
        print("  3. Add to .env file: OPENWEATHER_API_KEY='YOUR_API_KEY_HERE'")
        return
 
    # Try to initialize the DB, but don't stop the whole pipeline if it fails —
    # the CSV backup will still capture the data
    if not init_database():
        print("  [ERROR] Database initialization failed — saving to CSV only")
 
    # Step 1: Extract
    records = fetch_all_cities(CITIES)
 
    # If extraction returned nothing (network issue, bad API key, etc.), stop here.
    # There's no point running transform or load on an empty list.
    if not records:
        print("  [ERROR] No data fetched — pipeline stopped")
        return
 
    # Step 2: Transform
    df = process_data(records)
    
    # Step 3: Load — write to both DB and CSV in parallel
    save_to_db(df)
    save_to_csv(df)
    
    # Print a summary to the console so I can verify the run at a glance
    print_summary(df)
 
 
if __name__ == "__main__":
    # I added --schedule and --hours flags so this script can also act as the
    # scheduler entrypoint — useful when running from the command line or in Docker
    if "--schedule" in sys.argv:
        from scheduler import start_scheduler
        hours = 6  # Default: run every 6 hours
        
        # Allow overriding the interval: python main.py --schedule --hours 2
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])
            
        start_scheduler(interval_hours=hours)
    else:
        # No flags → just run the pipeline once and exit
        main()
