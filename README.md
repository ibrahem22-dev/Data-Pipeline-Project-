# Weather Data Pipeline

A full-stack **ETL (Extract, Transform, Load) data pipeline** that fetches real-time weather data for 5 Israeli cities from the OpenWeatherMap API, processes it with pandas, stores it in a PostgreSQL database, and exposes it via a FastAPI REST API.

Built as a portfolio project to demonstrate end-to-end data engineering skills.

---

## Features

- **Extract:** Fetches live weather data from OpenWeatherMap API with robust error handling (timeouts, HTTP errors, connection errors)
- **Transform:** Processes raw data with pandas, adds calculated fields (`temp_diff`, `temp_category`)
- **Load:** Saves data to both PostgreSQL (primary) and CSV (backup)
- **REST API:** 6 FastAPI endpoints with Swagger documentation at `/docs`
- **Scheduler:** Automated pipeline runs every N hours using the `schedule` library
- **Testing:** 25 pytest tests across 3 test files

---

## Project Structure

```
Data-Pipeline-Project/
├── src/
│   ├── main.py          # Entry point — runs pipeline once or starts scheduler
│   ├── config.py        # Configuration and environment variables
│   ├── extract.py       # Fetches weather data from OpenWeatherMap API
│   ├── transform.py     # Processes and enriches data with pandas
│   ├── load.py          # Saves data to PostgreSQL and CSV
│   ├── database.py      # Database connection and table setup (SQLAlchemy)
│   ├── scheduler.py     # Automated scheduling (every N hours)
│   ├── api.py           # FastAPI REST API (6 endpoints)
│   ├── schemas.py       # Pydantic response models
│   └── tests/
│       ├── conftest.py       # pytest configuration
│       ├── test_api.py       # 12 tests for API endpoints
│       ├── test_transform.py # 7 tests for data processing
│       └── test_extract.py   # 6 tests for data fetching (with mocks)
├── .env.example         # Template for environment variables
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11+ | Core language |
| requests | HTTP client for OpenWeatherMap API |
| pandas | Data processing and transformation |
| PostgreSQL 18 | Persistent data storage |
| SQLAlchemy + psycopg2 | Python ↔ Database connection |
| schedule | Task scheduling (Windows-compatible) |
| FastAPI + uvicorn | REST API server |
| Pydantic | Data validation and serialization |
| pytest + httpx | Testing |
| python-dotenv | Environment variable management |

---

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/ibrahem22-dev/Data-Pipeline-Project-
cd Data-Pipeline-Project-
```

### 2. Create a virtual environment
```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
```bash
copy .env.example .env
# Edit .env and add your OpenWeatherMap API key and database credentials
```

### 5. Create the PostgreSQL database
```sql
CREATE DATABASE weather_pipeline;
```

---

## Usage

### Run the pipeline once
```bash
cd src
python main.py
```

### Start the scheduler (runs every 6 hours)
```bash
cd src
python main.py --schedule
```

### Custom interval (e.g., every 2 hours)
```bash
cd src
python main.py --schedule --hours 2
```

### Start the API server
```bash
cd src
uvicorn api:app --reload
```
Then visit `http://127.0.0.1:8000/docs` for the Swagger UI.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | API info and endpoint list |
| GET | `/weather` | All readings (with `limit`/`offset` pagination) |
| GET | `/weather/stats` | Aggregated stats per city |
| GET | `/weather/latest` | Latest reading per city |
| GET | `/weather/{city}` | Readings for a specific city |
| POST | `/weather/fetch` | Manually trigger the ETL pipeline |

---

## Running Tests

```bash
cd src
pytest tests/ -v
```

---

## Cities Tracked

| City | Region |
|---|---|
| Nazareth | Northern Israel |
| Tel Aviv | Coastal (Mediterranean) |
| Haifa | Northern coast |
| Jerusalem | Mountain |
| Beer Sheva | Desert (Negev) |

---

## Author

**Ibrahim Abu Nasser** — CS & Math student  
GitHub: [@ibrahem22-dev](https://github.com/ibrahem22-dev)
