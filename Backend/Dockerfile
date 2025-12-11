# Base image
FROM python:3.11-slim

# Working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY . .

# Environment variables
ENV PORT=8080
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/service_account.json

# Expose port
EXPOSE 8080

CMD ["sh", "-c", "uvicorn myapi:app --host 0.0.0.0 --port $PORT"]















