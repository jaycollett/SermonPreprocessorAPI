FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create working directory
WORKDIR /app

# Copy scraper script
COPY sermon_scraper.py .

# Ensure Python can log in real-time
ENV PYTHONUNBUFFERED=1

# Run the script
CMD ["python", "sermon_scraper.py"]
