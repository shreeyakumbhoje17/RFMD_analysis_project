# Use official Python 3.11 slim image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all frontend files into the container
COPY . .

# Expose Streamlit port
EXPOSE 8080

# Command to run Streamlit
CMD ["streamlit", "run", "UI.py", "--server.port=8080", "--server.address=0.0.0.0"]

