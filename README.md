# SermonPreprocessorAPI

A microservice that monitors Trinity Christian Fellowship's teaching archive (the tcfky.com WordPress REST API), downloading sermon metadata and audio files. The service serves these sermons via a simple REST API.

## Overview

This application:
1. Periodically pages through the `tcf_teaching` REST API for new teachings of every type that has audio
2. Matches each teaching against existing rows (post ID, audio URL, file name, then title plus date) so nothing already stored is downloaded again, backfilling metadata onto matched rows
3. Downloads audio for genuinely new teachings that pass the ingest filters (gathering type, file extension, audio host). The filters never delete or change existing rows.
4. Stores information in a SQLite database
5. Provides an API to retrieve sermon information and audio files

## Technical Details

- Built with Flask for the API endpoints
- Reads the WordPress REST API (`/wp-json/wp/v2/tcf_teaching`) with a browser-style Accept header, which the host's firewall requires
- Adds metadata columns to the `sermons` table with an idempotent migration at startup
- Audio files are stored locally and tracked in a SQLite database
- Runs a background worker to automatically check for new sermons every 20 minutes

## API Endpoints

- **GET /sermons?date=YYYY-MM-DD**: Retrieves sermons fetched on or after the specified date. Each item has the original keys (`id`, `title`, `audio_url`, `categories`, `fetched_date`, `download_url`) plus `wp_post_id`, `speaker`, `series`, `gathering`, `book`, `scripture`, `translation`, `date_preached`, `duration`, `summary`, and `link`, which are null until a row has been matched to the archive
- **GET /download/<sermon_id>**: Downloads the audio file for a specific sermon

## Environment Variables

- `DB_PATH`: Path to the SQLite database (default: `/data/SermonProcessor.db`)
- `AUDIO_DIR`: Directory to store downloaded audio files (default: `/data/audiofiles`)
- `API_KEY`: Required API key for authentication
- `API_PAGE_DELAY_SECONDS`: Pause between API pages (default: `1.0`)
- `SCRAPER_DRY_RUN`: Set to `1` to make `python background_scraper.py` report only
- `EXCLUDED_GATHERINGS`: `tcf_gathering` names or slugs that are never downloaded, comma-separated and case-insensitive (default: `mars-hill-forum,ordination`). A post with several gatherings is skipped if any one of them is excluded. Set it to an empty string to exclude nothing.
- `ALLOWED_AUDIO_EXTENSIONS`: File extensions allowed for new downloads, taken from the URL path with the query string ignored (default: `mp3`). Empty allows any extension.
- `ALLOWED_AUDIO_HOSTS`: Hosts allowed for new downloads (default: `tcfky.com,www.tcfky.com`). Empty allows any host.

## Dry run and manual runs

```bash
python background_scraper.py --dry-run --db /data/SermonProcessor.db   # read-only report, no downloads
python background_scraper.py --max-downloads 10                        # one real cycle, capped
```

## Tests

```bash
uv run --no-project --with-requirements requirements-dev.txt pytest
```
