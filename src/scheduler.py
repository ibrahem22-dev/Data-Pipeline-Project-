import schedule
import time
from datetime import datetime
from extract import fetch_all_cities
from load import save_to_db, save_to_csv
from transform import process_data, print_summary
from database import init_database
from config import API_KEY, CITIES


def run_pipeline():
    print(f"\n{'#'*60}")
    print(f"  SCHEDULED RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    records = fetch_all_cities(CITIES)

    if not records:
        print("  [ERROR] No data fetched — skipping this run")
        return

    df = process_data(records)
    save_to_db(df)
    save_to_csv(df)
    print_summary(df)

    print(f"  [DONE] Next run scheduled automatically\n")


def start_scheduler(interval_hours: int = 6):
    if API_KEY == "":
        print("\n  [ERROR] API key not configured!")
        return

    if not init_database():
        print("  [ERROR] Database initialization failed")
        return

  
    print(f"  [SCHEDULER] Starting with {interval_hours}-hour interval")
    run_pipeline()

  
    schedule.every(interval_hours).hours.do(run_pipeline)

    print(f"  [SCHEDULER] Running every {interval_hours} hours")
    print(f"  [SCHEDULER] Press Ctrl+C to stop\n")

  
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print(f"\n  [SCHEDULER] Stopped by user")


if __name__ == "__main__":
    start_scheduler(interval_hours=6)