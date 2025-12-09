# Use official Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir google-cloud-bigquery db-dtypes

# Copy the backend code
COPY . .

# Environment variable for Cloud Run port
ENV PORT=8080
EXPOSE $PORT

# Set the service account path (Cloud Run will mount the secret here)
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/service-account.json

CMD ["sh", "-c", "uvicorn myapi:app --host 0.0.0.0 --port $PORT"]








