import os
from dotenv import load_dotenv

load_dotenv()

APP_ENV = os.getenv("APP_ENV", "development")

API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CITIES = [
    "Nazareth,IL",
    "Tel Aviv,IL",
    "Haifa,IL",
    "Jerusalem,IL",
    "Beer Sheva,IL",
]

OUTPUT_FILE = "weather_data.csv"
