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
EXPOSE 8080

# Cloud Run secret mount (used by FastAPI via env)
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/my-backend-sa-key

# Start FastAPI with uvicorn
CMD ["uvicorn", "myapi:app", "--host", "0.0.0.0", "--port", "8080"]







