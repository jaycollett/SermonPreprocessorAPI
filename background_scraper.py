import os
import requests
import sqlite3
import logging
import time
import uuid
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Constants and configuration
BASE_URL = "https://tcfky.com/sermons/page/"
DB_PATH = os.getenv("DB_PATH", "/data/SermonProcessor.db")
AUDIO_DIR = os.getenv("AUDIO_DIR", "/data/audiofiles")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
}

# Ensure necessary directories exist
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(AUDIO_DIR, exist_ok=True)

def get_database_connection():
    """Open a connection to the existing SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    return conn, cursor

def download_audio(audio_url):
    """Download sermon audio and return the local file path."""
    try:
        file_name = os.path.basename(urlparse(audio_url).path)
        file_path = os.path.join(AUDIO_DIR, file_name)
        
        if os.path.exists(file_path):
            logging.info(f"🔄 Audio file already exists: {file_name}")
            return file_path

        response = requests.get(audio_url, headers=HEADERS, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            logging.info(f"✅ Downloaded: {file_name}")
            return file_path
        else:
            logging.error(f"❌ Failed to download {audio_url} (status: {response.status_code})")
            return None
    except Exception as e:
        logging.error(f"⚠️ Error downloading {audio_url}: {e}")
        return None

def scrape_page(page_num):
    """Scrape sermons from a given page."""
    url = f"{BASE_URL}{page_num}/"
    logging.info(f"📡 Fetching page {page_num}: {url}")

    try:
        response = requests.get(url, headers=HEADERS)
    except Exception as e:
        logging.error(f"⚠️ Request error for page {page_num}: {e}")
        return []
    
    if response.status_code != 200:
        logging.error(f"❌ Page {page_num} returned status {response.status_code}.")
        return []
    
    soup = BeautifulSoup(response.text, "html.parser")
    sermons = []
    
    # Loop through sermon blocks
    for sermon in soup.find_all("div", class_="fusion-post-timeline"):
        title_tag = sermon.find("h2", class_="entry-title")
        title = title_tag.get_text(strip=True) if title_tag else "Unknown Sermon"
        
        # Extract categories
        category_links = sermon.find_all("a", rel="category tag")
        categories = ", ".join([cat.get_text(strip=True) for cat in category_links]) if category_links else "Uncategorized"

        # Extract audio file URL
        audio_tag = sermon.find("audio", class_="wp-audio-shortcode")
        audio_url = audio_tag.find("source")["src"] if audio_tag and audio_tag.find("source") else None

        if audio_url:
            sermons.append((title, audio_url, categories))
    
    return sermons

def process_sermons(cursor, conn):
    """
    Scrape sermons from page 1 and process each one.
    It checks for duplicates (using a normalized file path derived from the audio URL)
    before downloading and inserting new sermons.
    If a duplicate is found (i.e. a corresponding DB record exists), it logs as info.
    If the audio file exists on disk but no DB record is present, it is overwritten.
    Enhanced error control and logging are in place for troubleshooting.
    """
    page_num = 1
    try:
        sermons = scrape_page(page_num)
    except Exception as e:
        logging.error("❌ Error scraping page %s: %s", page_num, e)
        return

    if not sermons:
        logging.info("✅ No sermons found on page 1.")
        return

    for title, audio_url, categories in sermons:
        try:
            # Normalize the file name from the audio URL (ignoring query parameters)
            parsed = urlparse(audio_url)
            file_name = os.path.basename(parsed.path)
            normalized_file_path = os.path.join(AUDIO_DIR, file_name)

            # Check for duplicate by normalized file_path in the database
            cursor.execute("SELECT COUNT(*) FROM sermons WHERE file_path = ?", (normalized_file_path,))
            exists = cursor.fetchone()[0]
            if exists:
                logging.info("🔄 Duplicate already stored: %s (File: %s)", title, normalized_file_path)
                continue

            # If the audio file exists on disk but there's no DB record, overwrite it.
            if os.path.exists(normalized_file_path):
                logging.info("Audio file %s exists on disk without a DB record. Removing to force re-download.", normalized_file_path)
                try:
                    os.remove(normalized_file_path)
                except Exception as e:
                    logging.error("Failed to remove existing file %s: %s", normalized_file_path, e)
                    continue

            # Log details of the sermon before processing
            logging.debug("Processing sermon: Title: %s | Audio URL: %s | Categories: %s", title, audio_url, categories)
            
            # Download the audio file (will download since the file was removed if it existed)
            downloaded_file_path = download_audio(audio_url)
            if not downloaded_file_path:
                logging.error("⚠️ Download failed for sermon '%s' with Audio URL: %s", title, audio_url)
                continue

            # If the downloaded file path differs from the normalized one, log a warning and use the downloaded value.
            if downloaded_file_path != normalized_file_path:
                logging.warning("Normalized file path (%s) differs from downloaded file path (%s) for sermon '%s'",
                                normalized_file_path, downloaded_file_path, title)
                normalized_file_path = downloaded_file_path

            fetched_date = time.strftime('%Y-%m-%d %H:%M:%S')
            sermon_id = str(uuid.uuid4())  # Generate a UUID for the sermon
            
            try:
                cursor.execute('''
                    INSERT INTO sermons (id, title, audio_url, file_path, categories, fetched_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (sermon_id, title, audio_url, normalized_file_path, categories, fetched_date))
                conn.commit()
                logging.info("✅ Inserted sermon: %s (ID: %s)", title, sermon_id)
            except sqlite3.IntegrityError as e:
                # If the IntegrityError is due to a duplicate, log as info
                if "UNIQUE constraint failed" in str(e):
                    logging.info("🔄 Sermon already exists (detected at insert): %s (File: %s)", title, normalized_file_path)
                    conn.rollback()
                else:
                    logging.error("❌ SQLite IntegrityError for sermon '%s'. Audio URL: %s, File: %s. Error: %s",
                                  title, audio_url, normalized_file_path, e)
                    conn.rollback()
            except Exception as e:
                logging.error("❌ Unexpected error during insertion of sermon '%s'. Audio URL: %s, File: %s. Error: %s",
                              title, audio_url, normalized_file_path, e)
                conn.rollback()
        except Exception as e:
            logging.error("❌ Error processing sermon '%s' with Audio URL: %s. Error: %s", title, audio_url, e)

if __name__ == "__main__":
    conn, cursor = get_database_connection()
    logging.info("✅ Database connection established. Starting single scraping cycle.")

    try:
        logging.info("🔍 Starting scraping cycle...")
        process_sermons(cursor, conn)
        logging.info("✅ Scraping cycle complete.")
    except Exception as e:
        logging.error(f"⚠️ Unexpected error: {e}")
    finally:
        conn.close()
        logging.info("💾 Database connection closed.")
