FROM python:3.13-slim

# Install system-level dependencies required by psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer-cache friendly)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# All imports in this project are flat (e.g. "from extract import ...")
# so the working directory must be src/ at runtime.
WORKDIR /app/src

# Default: run the FastAPI server (overridden per-service in docker-compose)
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
