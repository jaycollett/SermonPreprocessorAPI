# SermonPreprocessorAPI

A microservice that monitors Trinity Christian Fellowship's podcast feed, downloading sermon metadata and audio files. The service serves these sermons via a simple REST API.

## Overview

This application:
1. Periodically checks the TCF podcast XML feed for new sermons
2. Downloads sermon metadata and audio files
3. Stores information in a SQLite database
4. Provides an API to retrieve sermon information and audio files

## Technical Details

- Built with Flask for the API endpoints
- Uses Python's ElementTree for XML parsing of the podcast feed
- Audio files are stored locally and tracked in a SQLite database
- Runs a background worker to automatically check for new sermons every 20 minutes

## API Endpoints

- **GET /sermons?date=YYYY-MM-DD**: Retrieves sermons fetched on or after the specified date
- **GET /download/<sermon_id>**: Downloads the audio file for a specific sermon

## Environment Variables

- `DB_PATH`: Path to the SQLite database (default: `/data/SermonProcessor.db`)
- `AUDIO_DIR`: Directory to store downloaded audio files (default: `/data/audiofiles`)
- `API_KEY`: Required API key for authentication