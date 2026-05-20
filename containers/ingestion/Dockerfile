FROM python:3.11-slim

WORKDIR /app

# Copy dependency configurations first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion application logic
COPY . .

# Define execution entrypoint
CMD ["python", "ingest_data.py"]
