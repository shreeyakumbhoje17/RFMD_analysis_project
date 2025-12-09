# Use official Python 3.11 slim image
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first to leverage cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all frontend files
COPY . .

# Expose Streamlit port
EXPOSE 8080

# Environment variables for Cloud Run
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_ENABLE_CORS=false
ENV STREAMLIT_SERVER_PORT=8080

# Command to run Streamlit
CMD ["streamlit", "run", "UI.py", "--server.port=8080", "--server.address=0.0.0.0"]


