import os
APP_ENV = os.getenv("APP_ENV", "development")

API_KEY = os.environ.get("OPENWEATHER_API_KEY", "YOUR_API_KEY_HERE")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CITIES = [
    "Nazareth,IL",     
    "Tel Aviv,IL",      
    "Haifa,IL",        
    "Jerusalem,IL",      
    "Beer Sheva,IL",     
]
CITIES = [
    "Nazareth,IL",      
    "Tel Aviv,IL",     
    "Haifa,IL",        
    "Jerusalem,IL",      
    "Beer Sheva,IL",    
]

OUTPUT_FILE = "weather_data.csv"