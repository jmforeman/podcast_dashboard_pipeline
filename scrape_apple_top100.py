import requests
import datetime
import sqlite3
import os
import json
import logging
from typing import List, Dict, Optional, Any # For type hinting

# --- Configuration ---
# Apple's RSS Feed Generator URL structure
APPLE_API_BASE_URL_TEMPLATE = "https://rss.marketingtools.apple.com/api/v2/{region}/podcasts/top/{limit}/podcasts.json"
DEFAULT_APPLE_REGION = "us"
DEFAULT_LIMIT = 100
PLATFORM_NAME_APPLE = "Apple" # Specific constant for Apple
DATABASE_NAME = "podcasts.db" # Shared constant (same as Spotify script)
LOG_LEVEL = logging.INFO # Change to logging.DEBUG for more verbose output

# --- Setup Logging ---
# BasicConfig should ideally be called only once if running multiple scrapers
# in sequence within the same execution. If run as separate scripts, this is fine.
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_apple_top_podcasts(
    region: str = DEFAULT_APPLE_REGION,
    limit: int = DEFAULT_LIMIT
) -> List[Dict[str, Any]]:
    """
    Scrapes the top podcasts from Apple's public RSS feed generator API for a given region.

    Args:
        region: The two-letter country code for the chart region (e.g., 'us', 'gb').
        limit: The number of top podcasts to fetch (e.g., 100).

    Returns:
        A list of dictionaries, each representing a podcast entry, or an empty list on failure.
    """
    url = APPLE_API_BASE_URL_TEMPLATE.format(region=region, limit=limit)
    logging.info(f"Requesting Apple chart data from: {url}")
    records = []
    try:
        response = requests.get(url, timeout=10) # Added timeout
        logging.info(f"HTTP status: {response.status_code}")
        # Log response snippet only if debugging
        logging.debug(f"Response snippet: {response.text[:200]}...")
        response.raise_for_status() # Raise HTTPError for bad status codes

        data = response.json()

        # Safely access nested data
        feed_data = data.get("feed")
        if not feed_data or not isinstance(feed_data, dict):
             logging.error("API response missing 'feed' object or it's not a dictionary.")
             return []

        results = feed_data.get("results")
        if not results or not isinstance(results, list):
             logging.error("API response missing 'results' list within 'feed' or it's not a list.")
             return []

        logging.info(f"Parsed {len(results)} items from API response.")
        today = str(datetime.date.today())

        for i, pod_data in enumerate(results[:limit]): # Ensure we don't exceed the requested limit
             if not isinstance(pod_data, dict):
                  logging.warning(f"Skipping item at index {i}, expected dict, got {type(pod_data)}")
                  continue

             records.append({
                 "platform": PLATFORM_NAME_APPLE,
                 "rank": i + 1, # Rank based on position in the list
                 "title": pod_data.get("name"), # Key is 'name' in Apple's API
                 "podcast_id": pod_data.get("id"), # Key is 'id' in Apple's API
                 "date": today
             })

    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed: {e}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON response: {e}")
        logging.debug(f"Response text: {response.text[:500]}...")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred during scraping: {e}")
        return []

    return records

def save_chart_data_to_db(records: List[Dict[str, Any]], db_path: str = DATABASE_NAME):
    """
    Saves scraped podcast chart records (from any platform) to the SQLite database.
    (This function is identical to the improved Spotify one and can be reused)

    Args:
        records: A list of podcast record dictionaries.
        db_path: The path to the SQLite database file.
    """
    if not records:
        logging.warning("No records provided to save.")
        return

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Top100Lists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    title TEXT,
                    podcast_id TEXT, -- Store as TEXT as IDs can be alphanumeric
                    date TEXT NOT NULL,
                    UNIQUE(platform, rank, date)
                )
            ''')
            # Optional: Add index for performance
            # cursor.execute('CREATE INDEX IF NOT EXISTS idx_platform_rank_date ON Top100Lists (platform, rank, date);')

            insert_count = 0
            ignore_count = 0
            for r in records:
                # Validate required keys before attempting insert
                required_keys = ["platform", "rank", "date"]
                if not all(key in r for key in required_keys):
                     logging.error(f"Record missing required keys: {r}. Skipping.")
                     continue
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO Top100Lists(platform, rank, title, podcast_id, date)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (r["platform"], r["rank"], r.get("title"), r.get("podcast_id"), r["date"])) # Use .get for optional fields
                    if cursor.rowcount > 0:
                        insert_count += 1
                    else:
                        ignore_count += 1
                except sqlite3.Error as e:
                     logging.error(f"Failed to insert record: {r} - Error: {e}")
                except KeyError as e: # Should be caught by the check above, but as fallback
                    logging.error(f"Missing key {e} in record: {r}")


            logging.info(f"Database operation complete for {records[0]['platform']} data. Inserted: {insert_count}, Ignored (duplicates): {ignore_count}")

    except sqlite3.Error as e:
        logging.error(f"Database error connecting or creating table: {e}")


if __name__ == "__main__":
    logging.info("Starting Apple Top Podcasts scrape...")
    # Using default region 'us' and limit 100
    scraped_data_apple = scrape_apple_top_podcasts()

    if scraped_data_apple:
        # Use the generic save function
        save_chart_data_to_db(scraped_data_apple)
        logging.info(f"Attempted to save {len(scraped_data_apple)} Apple Podcasts rows.")
    else:
        logging.warning("Apple Podcasts scraping returned no data. Nothing saved.")

    logging.info("Script finished.")