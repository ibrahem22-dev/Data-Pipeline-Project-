import os
from dotenv import load_dotenv

# Load environment variables from .env file
# I learned the hard way that hardcoding the API key blocks GitHub pushes!
# Always use python-dotenv to keep secrets safe.
load_dotenv()

# Application environment (development/production)
APP_ENV = os.getenv("APP_ENV", "development")

# OpenWeatherMap API settings
# Fetching the API key from the environment variables
API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Target cities in Israel chosen for geographic diversity (coastal, mountain, desert)
CITIES = [
    "Nazareth,IL",
    "Tel Aviv,IL",
    "Haifa,IL",
    "Jerusalem,IL",
    "Beer Sheva,IL",
]

# Backup CSV file path
OUTPUT_FILE = "weather_data.csv"

# Database Configuration
# Note: Using 127.0.0.1 instead of localhost to force TCP connection on Windows.
# If we use localhost, psycopg2 tries to use a Unix socket and fails.
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:your_password@127.0.0.1:5432/weather_pipeline"
)
