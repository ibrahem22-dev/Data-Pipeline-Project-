# Weather Data Pipeline

Automated pipeline that fetches weather data from OpenWeatherMap API, processes it with pandas, and stores it for analysis.

## Quick start

```bash
# 1. Clone and enter the project
git clone https://github.com/YOUR_USER/weather-data-pipeline.git
cd weather-data-pipeline

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure API key
cp .env.example .env
# Edit .env and add your OpenWeatherMap API key

# 5. Set the environment variable and run
export OPENWEATHER_API_KEY='your_key_here'
python weather_pipeline.py
```

## Cities tracked

| City | Type | Why |
|------|------|-----|
| Nazareth | Inland/hill | Moderate continental climate |
| Tel Aviv | Coastal | Mediterranean humidity |
| Haifa | Northern coast | Sea-level comparison |
| Jerusalem | Mountain | Altitude effects on temperature |
| Beer Sheva | Desert | Extreme heat, low humidity |

## Tech stack

- **Python 3.11+** — core language
- **requests** — HTTP client for API calls
- **pandas** — data processing and analysis

## Project status

- [x] API data fetching
- [x] Data processing with pandas
- [x] CSV export
- [ ] PostgreSQL storage
- [ ] Scheduled automation (cron)
- [ ] FastAPI REST endpoints
- [ ] Streamlit dashboard
- [ ] Docker containerization
- [ ] Cloud deployment