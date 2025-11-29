import os
import requests

API_KEY = os.getenv("YOUTUBE_API_KEY")
SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"


def get_channel_id_by_handle(handle: str):
    """@ハンドル（例: @IVEstarship）から channelId を取得する"""
    params = {
        "part": "snippet",
        "type": "channel",
        "q": handle,
        "key": API_KEY,
        "maxResults": 1,
    }

    resp = requests.get(SEARCH_URL, params=params, timeout=10)
    resp.raise_for_status()

    items = resp.json().get("items", [])
    if not items:
        return None

    return items[0]["snippet"]["channelId"]


def main():
    handle = "@STARSHIP_official"
    channel_id = get_channel_id_by_handle(handle)

    if channel_id:
        print("チャンネルID:", channel_id)
    else:
        print("チャンネルIDが見つかりませんでした。")


if __name__ == "__main__":
    main()
