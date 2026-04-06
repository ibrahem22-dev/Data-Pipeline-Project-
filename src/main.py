import sys
from extract import fetch_all_cities
from load import save_to_db, save_to_csv
from transform import process_data, print_summary
from database import init_database
from config import API_KEY, CITIES
 
 
def main():
    """
    The main entry point for the weather data pipeline.
    This function orchestrates the entire ETL process (Extract, Transform, Load).
    """
    # Check if the API key is configured before starting the pipeline
    if API_KEY == "":
        print("\n  [ERROR] API key not configured!")
        print("  Steps to fix:")
        print("  1. Go to https://openweathermap.org/api and sign up (free)")
        print("  2. Copy your API key from the dashboard")
        print("  3. Add to .env file: OPENWEATHER_API_KEY='YOUR_API_KEY_HERE'")
        return
 
    # Ensure the database tables exist before trying to save data
    if not init_database():
        print("  [ERROR] Database initialization failed — saving to CSV only")
 
    # Step 1: Extract data from the OpenWeatherMap API
    records = fetch_all_cities(CITIES)
 
    # Stop the pipeline if extraction failed to avoid processing empty data
    if not records:
        print("  [ERROR] No data fetched — pipeline stopped")
        return
 
    # Step 2: Transform the raw data into a pandas DataFrame
    df = process_data(records)
    
    # Step 3: Load the processed data into both PostgreSQL and CSV
    save_to_db(df)
    save_to_csv(df)
    
    # Print a summary of the processed data for easy monitoring
    print_summary(df)
 
 
if __name__ == "__main__":
    """
    This block allows the script to be run from the command line.
    It supports a '--schedule' flag to run the pipeline periodically.
    """
    # Check if the '--schedule' flag was passed as a command-line argument
    if "--schedule" in sys.argv:
        from scheduler import start_scheduler
        hours = 6 # Default interval is 6 hours
        
        # Check if a custom interval was specified with the '--hours' flag
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])
            
        # Start the scheduler with the specified interval
        start_scheduler(interval_hours=hours)
    else:
        # If no flags were passed, run the pipeline once immediately
        main()
