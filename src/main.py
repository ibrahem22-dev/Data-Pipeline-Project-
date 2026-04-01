from extract import fetch_all_cities
from load import save_to_csv 
from transform import process_data, print_summary
from config import API_KEY, CITIES
import pandas as pd


def main():

    if API_KEY == "":
        print("\n  [ERROR] API key not configured!")
        print("  Steps to fix:")
        print("  1. Go to https://openweathermap.org/api and sign up (free)")
        print("  2. Copy your API key from the dashboard")
        print("  3. Add to .env file: OPENWEATHER_API_KEY='YOUR_API_KEY_HERE'")
        return

    records = fetch_all_cities(CITIES)

    if not records:
        print("  [ERROR] No data fetched — pipeline stopped")
        return

    df = process_data(records)

    save_to_csv(df)

    print_summary(df)



if __name__ == "__main__":
    main()
