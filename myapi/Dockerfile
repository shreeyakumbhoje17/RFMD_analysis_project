# Use official Python 3.11 slim image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Google Cloud BigQuery client library (if not already in requirements.txt)
RUN pip install --no-cache-dir google-cloud-bigquery

# Copy all backend files into the container
COPY . .

# Copy service account JSON (you'll mount this from host in dev or add securely in Cloud Run)
# Uncomment this line if you include the key in build (not recommended for prod):
# COPY path/to/service-account.json /app/service-account.json

# Set environment variable for Google credentials
# If running locally or in Cloud Run, you can override this with a mounted secret or env var
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service-account.json"

# Expose FastAPI port
EXPOSE 8000

# Command to run FastAPI with Uvicorn
CMD ["uvicorn", "myapi:app", "--host", "0.0.0.0", "--port", "8000"]

