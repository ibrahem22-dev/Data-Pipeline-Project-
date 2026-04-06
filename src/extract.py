import requests
import time
from datetime import datetime   
from config import API_KEY, BASE_URL, CITIES

def fetch_weather(city: str) -> dict | None:
    """
    Fetches current weather data for a specific city from the OpenWeatherMap API.
    Handles various connection and HTTP errors gracefully.
    """
    # Setup API parameters
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric", # Using metric for Celsius temperatures
        "lang": "en",
    }
    
    try:
        # Make the API request with a 10-second timeout
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()
        
        # Extract only the relevant fields we need for our database schema
        # Clean up the city name (e.g., "Tel Aviv,IL" -> "Tel Aviv")
        weather_record = {
            "city": city.split(",")[0],
            "temperature": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "wind_speed": data["wind"]["speed"],
            "description": data["weather"][0]["description"],
            "clouds": data["clouds"]["all"],
            "recorded_at": datetime.now().isoformat(),   
        }

        return weather_record

    # Specific error handling to make debugging easier
    except requests.exceptions.Timeout:
        print(f"  [TIMEOUT] {city}: the server did not respond within 10 seconds")
        return None

    except requests.exceptions.HTTPError as e:
        print(f"  [HTTP ERROR] {city}: {e.response.status_code} — {e.response.reason}")
        return None

    except requests.exceptions.ConnectionError:
        print(f"  [CONNECTION ERROR] {city}: check your internet connection")
        return None

    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] {city}: {e}")
        return None

    except KeyError as e:
        print(f"  [DATA ERROR] {city}: missing field {e} in API response")
        return None


def fetch_all_cities(cities: list[str]) -> list[dict]:
    """
    Iterates through the configured list of cities and fetches data for each.
    Includes a small delay between requests to avoid hitting API rate limits.
    """
    results = []
    success_count = 0
    fail_count = 0

    # Print a nice header for the extraction process
    print(f"\n{'='*60}")
    print(f"  Weather Pipeline — Fetching {len(cities)} cities")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    for i, city in enumerate(cities):
        print(f"  [{i+1}/{len(cities)}] Fetching {city}...", end=" ")

        record = fetch_weather(city)

        if record is not None:
            results.append(record)
            success_count += 1
            print(f"OK — {record['temperature']}°C, {record['description']}")
        else:
            fail_count += 1

        # Sleep for 1 second between requests to be polite to the API
        if i < len(cities) - 1:
            time.sleep(1)

    # Print a summary footer
    print(f"\n{'='*60}")
    print(f"  Done: {success_count} succeeded, {fail_count} failed")
    print(f"{'='*60}\n")

    return results
