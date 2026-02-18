import requests
import json
from datetime import date
import os
import logging
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context

# Configuration via Airflow Variables
API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
max_results = 50


@task
def get_playlist_id():
    try:
        # Try channels endpoint by handle (new YouTube handles)
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        # If handle param isn't supported, try forUsername next
        if response.status_code == 400:
            url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forUsername={CHANNEL_HANDLE}&key={API_KEY}"
            response = requests.get(url)

        # If request returned an error status, we'll attempt a search fallback below
        if response.status_code >= 400:
            logging.warning("Initial channels lookup failed (status %s), attempting search fallback", response.status_code)
            raise requests.exceptions.RequestException("channels lookup failed")

        data = response.json()
        items = data.get('items', [])

        # If still no items, try searching for the channel and then fetch by id
        if not items:
            search_url = f"https://youtube.googleapis.com/youtube/v3/search?part=snippet&type=channel&q={CHANNEL_HANDLE}&key={API_KEY}"
            r2 = requests.get(search_url)
            r2.raise_for_status()
            search_data = r2.json()
            search_items = search_data.get('items', [])
            if not search_items:
                logging.warning("No channel found for handle/username: %s", CHANNEL_HANDLE)
                return None
            # some search results put channelId under 'id' not snippet
            channel_id = search_items[0].get('id', {}).get('channelId') or search_items[0]['snippet'].get('channelId')
            if not channel_id:
                logging.warning("Search returned no channelId for %s", CHANNEL_HANDLE)
                return None
            url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&id={channel_id}&key={API_KEY}"
            r3 = requests.get(url)
            r3.raise_for_status()
            data3 = r3.json()
            items3 = data3.get('items', [])
            if not items3:
                logging.warning("No channel data returned for id %s", channel_id)
                return None
            return items3[0]['contentDetails']['relatedPlaylists']['uploads']

        channel_items = items[0]
        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlist_id

    except requests.exceptions.RequestException as e:
        logging.info("Attempting search fallback due to error: %s", e)
        try:
            search_url = f"https://youtube.googleapis.com/youtube/v3/search?part=snippet&type=channel&q={CHANNEL_HANDLE}&key={API_KEY}"
            r2 = requests.get(search_url)
            r2.raise_for_status()
            search_data = r2.json()
            search_items = search_data.get('items', [])
            if not search_items:
                logging.warning("No channel found for handle/username: %s", CHANNEL_HANDLE)
                return None
            channel_id = search_items[0].get('id', {}).get('channelId') or search_items[0]['snippet'].get('channelId')
            if not channel_id:
                logging.warning("Search returned no channelId for %s", CHANNEL_HANDLE)
                return None
            url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&id={channel_id}&key={API_KEY}"
            r3 = requests.get(url)
            r3.raise_for_status()
            data3 = r3.json()
            items3 = data3.get('items', [])
            if not items3:
                logging.warning("No channel data returned for id %s", channel_id)
                return None
            return items3[0]['contentDetails']['relatedPlaylists']['uploads']
        except Exception:
            logging.exception("Search fallback also failed")
            return None


@task
def get_video_ids(playlistID):
    if not playlistID:
        logging.warning("get_video_ids called with empty playlistID")
        return []
    video_ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlistID}&key={API_KEY}"
    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            pageToken = data.get('nextPageToken')
            if not pageToken:
                break
        return video_ids
    except requests.exceptions.RequestException:
        logging.exception("Failed to fetch video ids")
        return []


def batch_list(video_id_lst, batch_size=50):
    for i in range(0, len(video_id_lst), batch_size):
        yield video_id_lst[i: i + batch_size]


@task
def extract_video_data(video_id_lst):
    extracted_data = []
    base_url = "https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics"
    try:
        for video_id_batch in batch_list(video_id_lst):
            video_ids = ",".join(video_id_batch)
            url = f"{base_url}&id={video_ids}&key={API_KEY}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                video_info = {
                    'videoId': item['id'],
                    'title': item['snippet']['title'],
                    'publishedAt': item['snippet']['publishedAt'],
                    'viewCount': item['statistics'].get('viewCount', 0),
                    'likeCount': item['statistics'].get('likeCount', 0),
                    'commentCount': item['statistics'].get('commentCount', 0),
                    'duration': item['contentDetails']['duration']
                }
                extracted_data.append(video_info)
        return extracted_data
    except requests.exceptions.RequestException:
        logging.exception("Failed to extract video data")
        return []


@task
def save_to_json(data):
    """Save extracted data to the shared `/opt/airflow/data` directory.

    Uses Airflow runtime context to determine the DAG run date (ds). Falls
    back to today's date if the context is unavailable.
    """
    try:
        ctx = get_current_context()
        execution_date = ctx.get('ds') or str(date.today())
    except Exception:
        logging.exception('Could not get Airflow context; using today as execution date')
        execution_date = str(date.today())

    output_path = f"/opt/airflow/data/YT_data_{execution_date}.json"
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.info('Saved JSON to %s', output_path)
    except Exception:
        logging.exception('Failed to write JSON to %s', output_path)
