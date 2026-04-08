import requests
import time
from datetime import datetime   
from config import API_KEY, BASE_URL, CITIES

def fetch_weather(city: str) -> dict | None:
    """
    Fetches current weather data for a single city from the OpenWeatherMap API.
    Returns None if anything goes wrong — I handle each failure type separately
    so the logs actually tell me what broke instead of just "something failed".
    """
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric",  # Celsius — I'm in Israel, metric is the only sane choice
        "lang": "en",
    }
    
    try:
        # 10-second timeout — if the API hasn't responded by then, it's not going to
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # I only pull the fields I actually need for the DB schema.
        # The API returns a lot of noise (sunrise, sunset, visibility...) I don't care about.
        # Also stripping the country code from the city name — "Tel Aviv,IL" → "Tel Aviv"
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

    # I split these into separate except blocks on purpose — a timeout is a very
    # different problem from a 401 Unauthorized, and I want the logs to reflect that
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
        # This happens if OpenWeatherMap changes their response structure.
        # Unlikely, but worth catching explicitly rather than getting a cryptic crash.
        print(f"  [DATA ERROR] {city}: missing field {e} in API response")
        return None


def fetch_all_cities(cities: list[str]) -> list[dict]:
    """
    Loops through all configured cities and fetches weather data for each.
    I added a 1-second sleep between requests — free-tier API keys have rate limits
    and I'd rather be polite than get blocked.
    """
    results = []
    success_count = 0
    fail_count = 0

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

        # Skip the sleep after the last city — no point waiting if there's nothing after it
        if i < len(cities) - 1:
            time.sleep(1)

    print(f"\n{'='*60}")
    print(f"  Done: {success_count} succeeded, {fail_count} failed")
    print(f"{'='*60}\n")

    return results
