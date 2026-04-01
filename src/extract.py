import requests
import time
from datetime import datetime   
from config import API_KEY, BASE_URL, CITIES


def fetch_weather(city: str) -> dict | None:

    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric",
        "lang": "en",
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
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

        if i < len(cities) - 1:
            time.sleep(1)

    print(f"\n{'='*60}")
    print(f"  Done: {success_count} succeeded, {fail_count} failed")
    print(f"{'='*60}\n")

    return results
