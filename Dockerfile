FROM python:3.9-slim

WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the required files
COPY app.py .
COPY background_scraper.py .

# Set environment variables (including the API key for basic authentication) API KEY IS NOT REAL
ENV API_KEY=499817f8-623f-4fae-b828-8dc551aba9bb
ENV DB_PATH=/data/SermonProcessor.db
ENV AUDIO_DIR=/data/audiofiles

EXPOSE 5060

CMD ["python", "app.py"]
