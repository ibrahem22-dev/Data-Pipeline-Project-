import os
from dotenv import load_dotenv

# Load environment variables from .env file.
# I learned the hard way that hardcoding the API key blocks GitHub pushes!
# Always use python-dotenv to keep secrets safe.
load_dotenv()

# Lets me switch behaviour between local dev and production without changing code
APP_ENV = os.getenv("APP_ENV", "development")

# The API key lives in .env locally and in Render's environment variables in production.
# If it's missing, the pipeline will catch it early and print a clear error.
API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# I chose these 5 cities to get geographic diversity across Israel:
# Nazareth (north, inland), Tel Aviv (coast), Haifa (north coast),
# Jerusalem (mountain), Beer Sheva (south, desert).
# The ",IL" suffix helps the API disambiguate — there are cities named
# Jerusalem and Haifa in other countries too.
CITIES = [
    "Nazareth,IL",
    "Tel Aviv,IL",
    "Haifa,IL",
    "Jerusalem,IL",
    "Beer Sheva,IL",
]

# Local CSV backup path — used by save_to_csv() in load.py
OUTPUT_FILE = "weather_data.csv"

# Using 127.0.0.1 instead of "localhost" here because on Windows, psycopg2
# tries to connect via Unix socket when the hostname is "localhost" and fails.
# 127.0.0.1 forces a TCP connection and works on both Windows and Linux.
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:your_password@127.0.0.1:5432/weather_pipeline"
)
