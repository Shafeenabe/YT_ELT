import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")

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


if __name__ == "__main__":
    #print("get_playlist_id will be executed")
    get_playlist_id()

    #print("get_playlist_id will not be executed")
