import os
import requests
import logging

# Configure robust logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Configuration variables
API_KEY = os.getenv("API_KEY", "499817f8-623f-4fae-b828-8dc551aba9bb")  # Replace with your API key or set as environment variable
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:5060")
SERMON_DATE = "2025-02-01"  # Date filter in YYYY-MM-DD format

def fetch_sermons(date):
    """
    Call the /sermons API endpoint with the given date parameter.
    Returns the list of sermons if successful, or None on error.
    """
    url = f"{API_BASE_URL}/sermons"
    params = {"date": date}
    logging.info(f"Fetching sermons from {date} using URL: {url}")
    
    try:
        response = requests.get(url, params=params, auth=("api", API_KEY))
        if response.status_code == 200:
            logging.info("Successfully fetched sermons.")
            return response.json()
        else:
            logging.error(f"Error fetching sermons: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Exception during fetching sermons: {e}")
        return None

def download_audio(download_url, sermon_id):
    """
    Downloads the audio file from the given download URL using basic auth,
    and saves it locally as '<sermon_id>.mp3'.
    """
    logging.info(f"Downloading audio file from {download_url}")
    try:
        response = requests.get(download_url, stream=True, auth=("api", API_KEY))
        if response.status_code == 200:
            file_name = f"{sermon_id}.mp3"
            with open(file_name, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            logging.info(f"Audio file downloaded and saved as {file_name}")
        else:
            logging.error(f"Error downloading audio file: {response.status_code} - {response.text}")
    except Exception as e:
        logging.error(f"Exception during downloading audio: {e}")

def main():
    sermons = fetch_sermons(SERMON_DATE)
    if sermons is None:
        logging.error("Failed to fetch sermons.")
        return

    if not sermons:
        logging.info("No sermons found after the specified date.")
        return

    # Assuming the sermons are returned in descending order of fetched_date,
    # select the first sermon as the most recent one.
    most_recent = sermons[0]
    sermon_id = most_recent.get("id")
    download_url = most_recent.get("download_url")
    
    if not sermon_id or not download_url:
        logging.error("The most recent sermon does not contain a valid ID or download URL.")
        return

    logging.info(f"Most recent sermon: {most_recent.get('title')} (ID: {sermon_id})")
    download_audio(download_url, sermon_id)

if __name__ == "__main__":
    main()
