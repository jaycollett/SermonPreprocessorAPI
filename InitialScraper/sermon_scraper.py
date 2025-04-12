import os
import requests
import sqlite3
import logging
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Constants
PODCAST_FEED_URL = "https://tcfky.com/feed/podcast"
DB_PATH = os.getenv("DB_PATH", "/data/SermonProcessor.db")
AUDIO_DIR = os.getenv("AUDIO_DIR", "/data/audiofiles")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
}

# Ensure directories exist
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(AUDIO_DIR, exist_ok=True)

def initialize_database():
    """Check if the database exists, and if not, create it with the schema."""
    db_exists = os.path.exists(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if not db_exists:
        logging.info("🆕 Database file not found. Creating new database and applying schema.")
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sermons (
            id TEXT PRIMARY KEY, 
            title TEXT NOT NULL,
            audio_url TEXT NOT NULL UNIQUE,
            file_path TEXT NOT NULL UNIQUE,
            categories TEXT,
            fetched_date TEXT NOT NULL
        )
    ''')
    conn.commit()

    return conn, cursor

conn, cursor = initialize_database()
logging.info("✅ Database initialized")

def download_audio(audio_url):
    """Download sermon audio and return local file path."""
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

def fetch_podcast_feed():
    """Fetch and parse the podcast XML feed."""
    logging.info(f"📡 Fetching podcast feed: {PODCAST_FEED_URL}")
    
    try:
        response = requests.get(PODCAST_FEED_URL, headers=HEADERS)
    except Exception as e:
        logging.error(f"⚠️ Request error for podcast feed: {e}")
        return []
    
    if response.status_code != 200:
        logging.error(f"❌ Podcast feed returned status {response.status_code}.")
        return []

    try:
        # Parse XML feed
        root = ET.fromstring(response.content)
        
        # Find all item elements (each represents a sermon)
        items = root.findall('.//item')
        
        sermons = []
        for item in items:
            # Extract sermon title
            title_elem = item.find('./title')
            title = title_elem.text if title_elem is not None else "Unknown Sermon"
            
            # Extract audio URL (enclosure element with type="audio/mpeg")
            enclosure = item.find('./enclosure[@type="audio/mpeg"]')
            audio_url = enclosure.get('url') if enclosure is not None else None
            
            # Extract categories
            category_elems = item.findall('./category')
            categories = ", ".join([cat.text for cat in category_elems if cat.text]) if category_elems else "Uncategorized"
            
            # If we have an audio URL, add this sermon to our list
            if audio_url:
                sermons.append((title, audio_url, categories))
        
        logging.info(f"✅ Successfully parsed podcast feed. Found {len(sermons)} sermons.")
        return sermons
    
    except Exception as e:
        logging.error(f"❌ Error parsing podcast XML: {e}")
        return []

def process_sermons():
    """Process all sermons from the podcast feed."""
    sermons = fetch_podcast_feed()
    if not sermons:
        logging.info("✅ No sermons found in podcast feed.")
        return
    
    logging.info(f"🔍 Found {len(sermons)} sermons in the podcast feed")
    
    for title, audio_url, categories in sermons:
        try:
            # Normalize the file name from the audio URL
            file_name = os.path.basename(urlparse(audio_url).path)
            file_path = os.path.join(AUDIO_DIR, file_name)
            
            # Check for duplicates using multiple identifiers to avoid re-downloading
            # sermons that were downloaded with the old web scraping code
            
            # Check by audio_url (direct match with podcast URL)
            cursor.execute("SELECT COUNT(*) FROM sermons WHERE audio_url = ?", (audio_url,))
            exists_by_url = cursor.fetchone()[0]
            
            # Check by file_path
            cursor.execute("SELECT COUNT(*) FROM sermons WHERE file_path = ?", (file_path,))
            exists_by_path = cursor.fetchone()[0]
            
            # Check by title (for cases where URLs changed but content is the same)
            cursor.execute("SELECT COUNT(*) FROM sermons WHERE title = ?", (title,))
            exists_by_title = cursor.fetchone()[0]
            
            if exists_by_url or exists_by_path or exists_by_title:
                logging.info(f"🔄 Duplicate sermon detected: {title}")
                if exists_by_url:
                    logging.debug(f"  - Matched by audio URL")
                if exists_by_path:
                    logging.debug(f"  - Matched by file path: {file_path}")
                if exists_by_title:
                    logging.debug(f"  - Matched by title")
                continue
            
            # Download the audio file
            downloaded_file_path = download_audio(audio_url)
            if not downloaded_file_path:
                logging.error(f"⚠️ Download failed for sermon '{title}' with URL: {audio_url}")
                continue
            
            fetched_date = time.strftime('%Y-%m-%d %H:%M:%S')
            sermon_id = str(uuid.uuid4())  # Generate UUID for sermon ID
            
            # Insert into database
            cursor.execute('''
                INSERT INTO sermons (id, title, audio_url, file_path, categories, fetched_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (sermon_id, title, audio_url, downloaded_file_path, categories, fetched_date))
            conn.commit()
            
            logging.info(f"✅ Inserted: {title} (ID: {sermon_id})")
        except Exception as e:
            logging.error(f"❌ Error processing sermon '{title}': {e}")
            continue

process_sermons()
conn.close()
logging.info("\n🎉 Podcast Feed Processing & Download Complete! Check SermonProcessor.db and audiofiles directory.")