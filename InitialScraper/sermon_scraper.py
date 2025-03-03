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

# Constants
BASE_URL = "https://tcfky.com/sermons/page/"
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
            logging.error(f"❌ Failed to download {audio_url}")
            return None
    except Exception as e:
        logging.error(f"⚠️ Error downloading {audio_url}: {e}")
        return None

def scrape_page(page_num):
    """Scrape sermons from a given page."""
    url = f"{BASE_URL}{page_num}/"
    logging.info(f"📡 Fetching page {page_num}: {url}")

    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        logging.error(f"❌ Page {page_num} returned status {response.status_code}. Stopping.")
        return []
    
    soup = BeautifulSoup(response.text, "html.parser")
    sermons = []
    
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

def process_sermons():
    """Scrape and store sermons from pages 1-37."""
    for page_num in range(1, 38):  # Pages 1 to 37
        sermons = scrape_page(page_num)
        if not sermons:
            logging.info(f"✅ No more sermons found on page {page_num}. Stopping.")
            break
        
        for title, audio_url, categories in sermons:
            file_path = download_audio(audio_url)
            if not file_path:
                continue

            fetched_date = time.strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute("SELECT COUNT(*) FROM sermons WHERE audio_url = ?", (audio_url,))
            exists = cursor.fetchone()[0]
            if exists:
                logging.info(f"🔄 Skipping duplicate: {title}")
                continue
            
            sermon_id = str(uuid.uuid4())  # Generate UUID v4 for the sermon
            cursor.execute('''
                INSERT INTO sermons (id, title, audio_url, file_path, categories, fetched_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (sermon_id, title, audio_url, file_path, categories, fetched_date))
            conn.commit()

            logging.info(f"✅ Inserted: {title} (ID: {sermon_id})")

process_sermons()
conn.close()
logging.info("\n🎉 HTML Scraping & Download Complete! Check SermonProcessor.db and audiofiles directory.")
 