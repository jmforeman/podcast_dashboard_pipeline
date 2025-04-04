import requests
import datetime
import sqlite3
import os
import json
import logging
from typing import List, Dict, Optional, Any # For type hinting

# --- Configuration ---
# Note: This API endpoint is not officially documented by Spotify and may change/break.
API_BASE_URL = "https://podcastcharts.byspotify.com/api/charts/top"
DEFAULT_REGION = "us"
PLATFORM_NAME = "Spotify"
DATABASE_NAME = "podcasts.db"
LOG_LEVEL = logging.INFO # Change to logging.DEBUG for more verbose output

# --- Setup Logging ---
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_spotify_top100(region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    """
    Scrapes the top 100 podcasts from the unofficial Spotify charts API for a given region.

    Args:
        region: The two-letter country code for the chart region (e.g., 'us', 'gb').

    Returns:
        A list of dictionaries, each representing a podcast entry, or an empty list on failure.
    """
    url = f"{API_BASE_URL}?region={region}"
    logging.info(f"Requesting Spotify chart data from: {url}")
    records = []
    try:
        response = requests.get(url, timeout=10) # Added timeout
        logging.info(f"HTTP status: {response.status_code}")
        response.raise_for_status() # Raise an HTTPError for bad status codes (4xx or 5xx)

        items = response.json()
        if not isinstance(items, list):
             logging.error(f"Unexpected API response format. Expected a list, got {type(items)}")
             return []

        logging.info(f"Parsed {len(items)} items from API.")
        today = str(datetime.date.today())

        for i, pod_data in enumerate(items[:100]): # Process only the top 100
             if not isinstance(pod_data, dict):
                  logging.warning(f"Skipping item at index {i}, expected dict, got {type(pod_data)}")
                  continue

             show_uri = pod_data.get("showUri", "")
             podcast_id = show_uri.split(":")[-1] if show_uri else None # Handle potentially missing URI

             records.append({
                 "platform": PLATFORM_NAME,
                 "rank": i + 1,
                 "title": pod_data.get("showName"),
                 "podcast_id": podcast_id, # Store the extracted ID or None
                 "date": today
             })

    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed: {e}")
        return [] # Return empty list on request failure
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON response: {e}")
        logging.debug(f"Response text: {response.text[:500]}...") # Log part of the text if DEBUG level
        return [] # Return empty list on JSON error
    except Exception as e:
        logging.error(f"An unexpected error occurred during scraping: {e}")
        return []

    return records

def save_to_db(records: List[Dict[str, Any]], db_path: str = DATABASE_NAME):
    """
    Saves scraped podcast chart records to the SQLite database.

    Args:
        records: A list of podcast record dictionaries.
        db_path: The path to the SQLite database file.
    """
    if not records:
        logging.warning("No records provided to save.")
        return

    # Use context manager for connection handling
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Use IF NOT EXISTS for robustness
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Top100Lists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    title TEXT,
                    podcast_id TEXT,
                    date TEXT NOT NULL,
                    UNIQUE(platform, rank, date)
                )
            ''')
            # Consider adding an index for faster lookups if the table grows large
            # cursor.execute('CREATE INDEX IF NOT EXISTS idx_platform_rank_date ON Top100Lists (platform, rank, date);')

            insert_count = 0
            ignore_count = 0
            for r in records:
                try:
                    # Use INSERT OR IGNORE to handle the UNIQUE constraint gracefully
                    cursor.execute('''
                        INSERT OR IGNORE INTO Top100Lists(platform, rank, title, podcast_id, date)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (r["platform"], r["rank"], r["title"], r["podcast_id"], r["date"]))
                    if cursor.rowcount > 0:
                        insert_count += 1
                    else:
                        ignore_count += 1
                except sqlite3.Error as e:
                     logging.error(f"Failed to insert record: {r} - Error: {e}")
                except KeyError as e:
                    logging.error(f"Missing key {e} in record: {r}")


            # Commit happens automatically when 'with' block exits successfully
            logging.info(f"Database operation complete. Inserted: {insert_count}, Ignored (duplicates): {ignore_count}")

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")

if __name__ == "__main__":
    logging.info("Starting Spotify Top 100 scrape...")
    scraped_data = scrape_spotify_top100() # Uses default region 'us'

    if scraped_data:
        save_to_db(scraped_data) # Uses default DB name
        logging.info(f"Attempted to save {len(scraped_data)} Spotify rows.")
    else:
        logging.warning("Scraping returned no data. Nothing saved to the database.")

    logging.info("Script finished.")