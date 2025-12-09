# Use official Python 3.11 slim image
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir google-cloud-bigquery db-dtypes

# Copy backend code
COPY . .

# Use the port provided by Cloud Run
ENV PORT=8080
EXPOSE $PORT

# Run Uvicorn pointing to the module inside the Backend folder
CMD ["sh", "-c", "uvicorn myapi:app --host 0.0.0.0 --port $PORT"]



