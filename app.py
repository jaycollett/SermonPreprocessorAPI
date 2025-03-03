import os
import sqlite3
import logging
import threading
import sys
import time
from flask import Flask, request, jsonify, abort, send_from_directory
from functools import wraps
from datetime import datetime

# Configure verbose logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration from environment variables
DB_PATH = os.getenv("DB_PATH", "/data/SermonProcessor.db")
AUDIO_DIR = os.getenv("AUDIO_DIR", "/data/audiofiles")
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise Exception("API_KEY environment variable not set.")

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        logger.debug("Authorization header received: %s", auth)
        if not auth or auth.password != API_KEY:
            logger.warning("Unauthorized access attempt.")
            abort(401, description="Unauthorized: Invalid API key.")
        return f(*args, **kwargs)
    return decorated

@app.route('/sermons', methods=['GET'])
@require_api_key
def get_sermons():
    """
    GET /sermons?date=YYYY-MM-DD
    Returns sermons fetched on or after the specified date.
    Each sermon record includes a download_url for its audio file.
    """
    date_param = request.args.get("date")
    logger.debug("Received /sermons request with date parameter: %s", date_param)
    if not date_param:
        logger.error("Date parameter missing in /sermons request.")
        return jsonify({"error": "Missing date parameter. Expected format: YYYY-MM-DD"}), 400

    try:
        dt = datetime.strptime(date_param.strip(), "%Y-%m-%d")
        query_date = dt.strftime("%Y-%m-%d 00:00:00")
        logger.debug("Parsed query date: %s", query_date)
    except ValueError:
        logger.error("Invalid date format provided: %s", date_param)
        return jsonify({"error": "Invalid date format. Expected format: YYYY-MM-DD"}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        logger.debug("Querying database for sermons on or after: %s", query_date)
        cursor.execute(
            "SELECT * FROM sermons WHERE fetched_date >= ? ORDER BY fetched_date DESC",
            (query_date,)
        )
        rows = cursor.fetchall()
        sermons_list = []
        logger.info("Fetched %d sermons from the database.", len(rows))
        for row in rows:
            sermon_data = {
                "id": row["id"],
                "title": row["title"],
                "audio_url": row["audio_url"],
                "categories": row["categories"],
                "fetched_date": row["fetched_date"],
                "download_url": request.host_url.rstrip('/') + '/download/' + row["id"]
            }
            logger.debug("Sermon retrieved: %s", sermon_data)
            sermons_list.append(sermon_data)
        conn.close()
        return jsonify(sermons_list), 200
    except Exception as e:
        logger.exception("Error retrieving sermons from the database.")
        return jsonify({"error": str(e)}), 500

@app.route('/download/<sermon_id>', methods=['GET'])
@require_api_key
def download_sermon_audio(sermon_id):
    """
    GET /download/<sermon_id>
    Serves the audio file associated with the given sermon ID.
    """
    logger.debug("Received download request for sermon ID: %s", sermon_id)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT file_path FROM sermons WHERE id = ?", (sermon_id,))
        row = cursor.fetchone()
        conn.close()

        if row is None:
            logger.error("Sermon with ID %s not found.", sermon_id)
            return jsonify({"error": "Sermon not found"}), 404

        file_path = row["file_path"]
        directory = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        logger.info("Serving file '%s' from directory '%s' for sermon ID %s", filename, directory, sermon_id)
        return send_from_directory(directory, filename, as_attachment=True)
    except Exception as e:
        logger.exception("Error serving audio file for sermon ID: %s", sermon_id)
        return jsonify({"error": str(e)}), 500

def background_worker():
    """
    Background worker that scrapes sermons every 20 minutes.
    It calls the process_sermons() function from background_scraper.py.
    """
    logger.info("🟢 Sermon scraper worker started. Checking for new sermons every 20 minutes.")
    while True:
        logger.info("⏳ Worker sleeping for 20 minutes...")
        time.sleep(1200)  # 1200 seconds = 20 minutes
        logger.info("🔍 Worker waking up to check for new sermons...")
        try:
            from background_scraper import process_sermons, get_database_connection
            conn, cursor = get_database_connection()
            process_sermons(cursor, conn)
            conn.close()
            logger.info("✅ Sermon scraping cycle completed successfully.")
        except Exception as e:
            logger.error("❌ Worker error during sermon scraping: %s", e)


if __name__ == "__main__":
    worker_thread = threading.Thread(target=background_worker, daemon=True)
    worker_thread.start()

    app.run(host="0.0.0.0", port=5060, debug=True, use_reloader=False)

