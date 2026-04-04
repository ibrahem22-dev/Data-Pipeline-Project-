import sys
from extract import fetch_all_cities
from load import save_to_db, save_to_csv
from transform import process_data, print_summary
from database import init_database
from config import API_KEY, CITIES


def main():

    if API_KEY == "":
        print("\n  [ERROR] API key not configured!")
        print("  Steps to fix:")
        print("  1. Go to https://openweathermap.org/api and sign up (free)")
        print("  2. Copy your API key from the dashboard")
        print("  3. Add to .env file: OPENWEATHER_API_KEY='YOUR_API_KEY_HERE'")
        return

    if not init_database():
        print("  [ERROR] Database initialization failed — saving to CSV only")

    records = fetch_all_cities(CITIES)

    if not records:
        print("  [ERROR] No data fetched — pipeline stopped")
        return

    df = process_data(records)
    save_to_db(df)
    save_to_csv(df)
    print_summary(df)


if __name__ == "__main__":

    if "--schedule" in sys.argv:
        from scheduler import start_scheduler
        hours = 6
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])
        start_scheduler(interval_hours=hours)
    else:
        main()