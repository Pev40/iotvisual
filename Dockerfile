# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY conector.py .

# Expose port 5000
EXPOSE 5000

# Set environment variables
ENV FLASK_APP=conector.py
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "conector.py"]
