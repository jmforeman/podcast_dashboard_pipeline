import sqlite3
import requests
import time
import hashlib
import difflib
import json

# Podcast Index API credentials and base URL.
API_KEY = os.environ.get("PODCASTINDEX_API_KEY")
API_SECRET = os.environ.get("PODCASTINDEX_API_SECRET")

# Added check
if not API_KEY or not API_SECRET:
    print("Error: Podcast Index API keys not found in environment variables.")
    exit(1) # Exit if keys are missing
BASE_URL = "https://api.podcastindex.org/api/1.0/"

# --- Helper Functions ---

def get_headers():
    """Generates the required authentication headers for the API."""
    auth_date = str(int(time.time()))
    auth_string = API_KEY + API_SECRET + auth_date
    authorization = hashlib.sha1(auth_string.encode("utf-8")).hexdigest()
    headers = {
        "User-Agent": "PodcastDashboard/1.2 (Python Script)", # Updated version
        "X-Auth-Key": API_KEY,
        "X-Auth-Date": auth_date,
        "Authorization": authorization,
    }
    return headers

# --- Search Functions (Unchanged) ---

def search_byterm(query):
    """Search using 'search/byterm' (max 10 results) and pick best match by fuzzy ratio."""
    endpoint = "search/byterm"
    url = BASE_URL + endpoint
    params = {"q": query, "max": 10}
    headers = get_headers()
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"[byterm] Raw response for '{query}': {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        results = data.get("feeds") or data.get("results", [])
        if results:
            best_match = None
            best_ratio = 0
            for res in results:
                candidate_title = res.get("title_original", "") or res.get("title", "")
                if not candidate_title: continue
                ratio = difflib.SequenceMatcher(None, query.lower(), candidate_title.lower()).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = res
            print(f"[byterm] Best ratio for '{query}': {best_ratio:.2f} (Match: '{best_match.get('title') if best_match else 'None'}')")
            if best_match and best_ratio >= 0.4:
                return best_match
            else:
                print(f"[byterm] No good match found for '{query}' (Best ratio: {best_ratio:.2f})")
        else:
            print(f"[byterm] No results found for '{query}' in response.")
    except requests.exceptions.RequestException as e:
        print(f"[byterm] Request Exception for '{query}': {e}")
    except json.JSONDecodeError as e:
        print(f"[byterm] JSON Decode Error for '{query}': {e} - Response was: {response.text[:200]}...")
    except Exception as e:
        print(f"[byterm] General Exception for '{query}': {e}")
    return None

def search_bytitle(query):
    """Search using 'search/bytitle' (max 10 results) and pick best match by fuzzy ratio."""
    endpoint = "search/bytitle"
    url = BASE_URL + endpoint
    params = {"q": query, "max": 10} # Corrected parameter is 'q'
    headers = get_headers()
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"[bytitle] Raw response for '{query}': {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        feeds = data.get("feeds", [])
        if feeds:
            best_match = None
            best_ratio = 0
            for feed in feeds:
                candidate_title = feed.get("title", "")
                if not candidate_title: continue
                ratio = difflib.SequenceMatcher(None, query.lower(), candidate_title.lower()).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = feed
            print(f"[bytitle] Best ratio for '{query}': {best_ratio:.2f} (Match: '{best_match.get('title') if best_match else 'None'}')")
            if best_match and best_ratio >= 0.4:
                return best_match
            else:
                print(f"[bytitle] No good match found for '{query}' (Best ratio: {best_ratio:.2f})")
        else:
            print(f"[bytitle] No results found for '{query}' in response.")
    except requests.exceptions.RequestException as e:
        print(f"[bytitle] Request Exception for '{query}': {e}")
    except json.JSONDecodeError as e:
        print(f"[bytitle] JSON Decode Error for '{query}': {e} - Response was: {response.text[:200]}...")
    except Exception as e:
        print(f"[bytitle] General Exception for '{query}': {e}")
    return None

def search_podcast_combined(query):
    """Try searching using byterm first; if that fails or match is poor, fall back to bytitle."""
    print(f"\n--- Searching combined for: '{query}' ---")
    result = search_byterm(query)
    if not result:
        print(f"-> Falling back to search/bytitle for: '{query}'")
        result = search_bytitle(query)

    if result:
        print(f"--> Combined search found candidate: ID {result.get('id')}, Title: {result.get('title')}")
        return result
    else:
        print(f"--> Combined search FAILED for: '{query}'")
        return None

# --- Detail Fetching Functions ---

def get_full_podcast_details_by_feed_id(feed_id):
    """Retrieve full podcast details using 'podcasts/byfeedid'."""
    endpoint = "podcasts/byfeedid"
    url = BASE_URL + endpoint
    params = {"id": feed_id, "pretty": 1}
    headers = get_headers()
    print(f"--- Fetching details by Feed ID: {feed_id} ---")
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"[byfeedid] Raw response for ID {feed_id}: {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        if data.get("feed"):
            print(f"[byfeedid] Successfully fetched details for Feed ID: {feed_id}")
            return data["feed"]
        else:
            print(f"[byfeedid] Response OK, but no 'feed' data found for Feed ID: {feed_id}. Response: {data}")
            return None
    except requests.exceptions.HTTPError as e:
        print(f"[byfeedid] HTTP Error {e.response.status_code} for Feed ID {feed_id}: {e.response.text[:200]}...")
    except requests.exceptions.RequestException as e:
        print(f"[byfeedid] Request Exception for Feed ID {feed_id}: {e}")
    except json.JSONDecodeError as e:
        print(f"[byfeedid] JSON Decode Error for Feed ID {feed_id}: {e} - Response was: {response.text[:200]}...")
    except Exception as e:
        print(f"[byfeedid] General Exception for Feed ID {feed_id}: {e}")
    return None

def get_full_podcast_details_by_feed_url(feed_url):
    """Retrieve full podcast details using 'podcasts/byfeedurl'."""
    endpoint = "podcasts/byfeedurl"
    url = BASE_URL + endpoint
    params = {"url": feed_url, "pretty": 1}
    headers = get_headers()
    print(f"--- Fetching details by Feed URL: {feed_url[:50]}... ---")
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"[byfeedurl] Raw response for URL {feed_url[:50]}...: {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        if data.get("feed"):
            print(f"[byfeedurl] Successfully fetched details for Feed URL: {feed_url[:50]}...")
            return data["feed"]
        else:
            print(f"[byfeedurl] Response OK, but no 'feed' data found for Feed URL: {feed_url[:50]}... Response: {data}")
            return None
    except requests.exceptions.HTTPError as e:
        print(f"[byfeedurl] HTTP Error {e.response.status_code} for URL {feed_url[:50]}...: {e.response.text[:200]}...")
    except requests.exceptions.RequestException as e:
        print(f"[byfeedurl] Request Exception for URL {feed_url[:50]}...: {e}")
    except json.JSONDecodeError as e:
        print(f"[byfeedurl] JSON Decode Error for URL {feed_url[:50]}...: {e} - Response was: {response.text[:200]}...")
    except Exception as e:
        print(f"[byfeedurl] General Exception for URL {feed_url[:50]}...: {e}")
    return None

# *** RENAMED function and MODIFIED return value ***
def get_latest_episode_info(feed_id):
    """
    Fetches the latest 10 episodes for a feed ID.
    Returns a tuple: (average_duration_seconds, latest_episode_title)
    Returns (None, None) if calculation fails or no episodes found.
    """
    endpoint = "episodes/byfeedid"
    url = BASE_URL + endpoint
    params = {"id": feed_id, "max": 10, "pretty": 1} # Request only the 10 most recent
    headers = get_headers()

    print(f"--- Fetching latest episode info for Feed ID: {feed_id} ---")

    latest_episode_title = None
    average_duration = None

    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"[episodes/latest] Raw response for {feed_id}: {response.text[:150]}...")
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])

        if not items:
            print(f"[episodes/latest] No episodes found for Feed ID: {feed_id}")
            return None, None # Return None for both values

        # *** Get title of the latest episode (first in the list) ***
        latest_episode_title = items[0].get("title")
        print(f"[episodes/latest] Latest episode title: '{latest_episode_title}'")

        # Calculate average duration
        total_duration = 0
        valid_episode_count = 0
        for episode in items:
            duration = episode.get("duration")
            if duration and isinstance(duration, int) and duration > 0:
                total_duration += duration
                valid_episode_count += 1

        if valid_episode_count > 0:
            average_duration = int(total_duration / valid_episode_count) # Return integer seconds
            print(f"--- Avg duration (last {valid_episode_count}) for {feed_id}: {average_duration} sec ---")
        else:
            print(f"--- No valid durations found in the last {len(items)} episodes for {feed_id} ---")
            # average_duration remains None

        return average_duration, latest_episode_title

    except requests.exceptions.HTTPError as e:
         print(f"[episodes/latest] HTTP Error {e.response.status_code} for Feed ID {feed_id}: {e.response.text[:150]}...")
    except requests.exceptions.RequestException as e:
        print(f"[episodes/latest] Request Exception for feed {feed_id}: {e}")
    except json.JSONDecodeError as e:
         print(f"[episodes/latest] JSON Decode Error for feed {feed_id}: {e} - Response: {response.text[:150]}...")
    except Exception as e:
        print(f"[episodes/latest] General Exception for feed {feed_id}: {e}")

    # Return None for both if any error occurred during fetch/processing
    return None, None


# --- Main Database Update Function ---

def update_all_podcast_details():
    """Fetches podcast titles, searches for them, gets full details
       (including avg duration & title of last episode), and updates the DB."""
    conn = None
    try:
        conn = sqlite3.connect('podcasts.db')
        cursor = conn.cursor()

        # Drop and recreate the Podcasts table with the new columns.
        cursor.execute("DROP TABLE IF EXISTS Podcasts")
        # *** ADDED latest_episode_title column ***
        cursor.execute('''
            CREATE TABLE Podcasts (
                podcast_id INTEGER PRIMARY KEY,
                title TEXT,
                description TEXT,
                feed_url TEXT,
                image_url TEXT,
                episode_count INTEGER,
                avg_duration_last_10 INTEGER,
                latest_episode_title TEXT, -- Added column for latest episode title
                last_update_time INTEGER,
                categories TEXT,
                podcast_guid TEXT,
                original_url TEXT
            )
        ''')
        conn.commit()

        # Get distinct podcast titles from Top100Lists.
        try:
            cursor.execute("SELECT DISTINCT title FROM Top100Lists")
            titles = [row[0] for row in cursor.fetchall()]
        except sqlite3.OperationalError as e:
            print(f"Error accessing Top100Lists table: {e}")
            print("Please ensure the 'Top100Lists' table exists and has a 'title' column.")
            return

        print(f"\nFound {len(titles)} unique podcast titles from Top100Lists.")
        processed_count = 0

        for title in titles:
            processed_count += 1
            print(f"\n======= Processing {processed_count}/{len(titles)}: '{title}' =======")

            # 1. Search
            candidate = search_podcast_combined(title)
            if not candidate:
                print(f"SKIPPING: No candidate found for '{title}' via search.")
                time.sleep(1)
                continue

            # 2. Get Full Details (by Feed ID or URL)
            candidate_feed_id = candidate.get("id")
            full_details = None
            if candidate_feed_id:
                full_details = get_full_podcast_details_by_feed_id(candidate_feed_id)
            else:
                 print(f"WARNING: Candidate for '{title}' found, but missing 'id'. Candidate data: {candidate}")

            if not full_details:
                feed_url_from_candidate = candidate.get("url") or candidate.get("originalUrl")
                if feed_url_from_candidate:
                    print(f"-> Feed ID fetch failed or missing, falling back to Feed URL for '{title}'")
                    full_details = get_full_podcast_details_by_feed_url(feed_url_from_candidate)
                else:
                    print(f"WARNING: Cannot fall back to feed URL for '{title}', URL missing in candidate: {candidate}")

            # 3. Process Details and Get Latest Episode Info
            if full_details:
                feed_id = full_details.get("id")
                podcast_title = full_details.get("title")
                description = full_details.get("description")
                feed_url = full_details.get("url")
                original_url = full_details.get("originalUrl")
                image_url = full_details.get("image") or full_details.get("artwork")
                episode_count = full_details.get("episodeCount")
                last_update_time = full_details.get("lastUpdateTime")
                categories_dict = full_details.get("categories")
                podcast_guid = full_details.get("podcastGuid")

                # *** Get latest episode info (duration and title) ***
                avg_dur_10 = None
                latest_title = None
                if feed_id:
                    # Call the RENAMED function and unpack the results
                    avg_dur_10, latest_title = get_latest_episode_info(feed_id)
                    # time.sleep(0.2) # Optional small delay
                else:
                     print(f"WARNING: Cannot get latest episode info for '{title}', Feed ID missing in full_details.")

                # Fallbacks for URLs
                if not feed_url: feed_url = candidate.get("url")
                if not original_url: original_url = candidate.get("originalUrl")

                # Categories to JSON
                categories_json = None
                if categories_dict and isinstance(categories_dict, dict):
                     try:
                         categories_json = json.dumps(categories_dict)
                     except TypeError:
                         print(f"Warning: Could not serialize categories for feed ID {feed_id}: {categories_dict}")
                         categories_json = "{}"
                elif isinstance(categories_dict, str):
                     categories_json = categories_dict

                print(f"+++ Preparing DB insert for Feed ID: {feed_id}, Title: '{podcast_title}', Episodes: {episode_count}, AvgDur10: {avg_dur_10}, LatestEp: '{latest_title}' +++")

                if feed_id:
                    try:
                        # *** Update INSERT statement with new column and value ***
                        cursor.execute('''
                            INSERT OR REPLACE INTO Podcasts
                            (podcast_id, title, description, feed_url, image_url, episode_count, avg_duration_last_10, latest_episode_title, last_update_time, categories, podcast_guid, original_url)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            feed_id,
                            podcast_title,
                            description,
                            feed_url,
                            image_url,
                            episode_count,
                            avg_dur_10,
                            latest_title, # Add the latest title here
                            last_update_time,
                            categories_json,
                            podcast_guid,
                            original_url
                        ))
                        conn.commit()
                        print(f"### SUCCESS: Updated details in DB for Feed ID: {feed_id} ('{title}') ###")
                    except sqlite3.Error as e:
                        print(f"!!! DATABASE ERROR for Feed ID {feed_id} ('{title}'): {e} !!!")
                        if conn: conn.rollback() # Check if conn exists before rollback
                else:
                    print(f"!!! SKIPPING DB INSERT: 'id' field missing in full_details for '{title}'. Details: {full_details} !!!")

            else:
                print(f"--- FAILED: Could not retrieve full details for '{title}' (Candidate ID: {candidate_feed_id}) ---")

            # Main delay between processing different podcast titles
            time.sleep(1.5) # Keep the main delay

    except sqlite3.Error as e:
        print(f"An error occurred with the database connection or operation: {e}")
    finally:
        if conn:
            conn.close()
            print("\n======= Database connection closed. =======")

    print("\n======= Podcast details update process complete. =======")


# --- Main Execution Block ---

if __name__ == "__main__":
    update_all_podcast_details()