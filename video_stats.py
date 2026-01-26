import requests
import json
from datetime import date
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")
max_results = 50

def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        # print(json.dumps(data, indent=4))

        channel_items = data['items'][0]
        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']

        print("Playlist ID:", channel_playlist_id)
        return channel_playlist_id

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
def get_video_ids(playlistID):
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
            # print(json.dumps(data, indent=4))

            for item in data['items']:
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            pageToken = data.get('nextPageToken')
            if not pageToken:
                break

        return video_ids
    
    except requests.exceptions.RequestException as e:
         raise e

def batch_list(video_id_lst, batch_size=50):
    for i in range(0, len(video_id_lst), batch_size):
        yield video_id_lst[i: i + batch_size]


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

            for item in data['items']:
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

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return []

    # print(f"An error occurred: {e}")
        #sreturn None
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, 'w', encoding='utf-8') as json_outfile:
        json.dump(extracted_data, json_outfile, ensure_ascii=False, indent=4)
if __name__ == "__main__":
    playlistid = get_playlist_id()
    video_ids = get_video_ids(playlistid)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
