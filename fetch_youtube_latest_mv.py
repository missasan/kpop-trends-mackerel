import os
import requests
import time
from typing import Optional

YOUTUBE_API_KEY_ENV = "YOUTUBE_API_KEY"
YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

MACKEREL_API_KEY_ENV = "MACKEREL_API_KEY"
MACKEREL_BASE_URL = "https://api.mackerelio.com/api/v0"
SERVICE_NAME = "kpop-trends"


def get_api_key() -> str:
    """環境変数から API キーを取得する"""
    api_key = os.getenv(YOUTUBE_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(f"環境変数 {YOUTUBE_API_KEY_ENV} が設定されていません。")
    return api_key


def get_mackerel_api_key() -> str:
    api_key = os.getenv(MACKEREL_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(f"環境変数 {MACKEREL_API_KEY_ENV} が設定されていません。")
    return api_key


from typing import Optional  # ← 追加してください

def post_service_metric(metric_name: str, value: float, timestamp: Optional[int] = None):
    """
    Mackerel のサービスメトリックを1点だけ送る。
    https://mackerel.io/ja/api-docs/entry/service-metrics を利用
    """
    if timestamp is None:
        timestamp = int(time.time())

    api_key = get_mackerel_api_key()

    url = f"{MACKEREL_BASE_URL}/services/{SERVICE_NAME}/tsdb"

    payload = [
        {
            "name": metric_name,
            "time": timestamp,
            "value": float(value),
        }
    ]

    headers = {
        "X-Api-Key": api_key,
        "Content-Type": "application/json",
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=10)
    resp.raise_for_status()


def search_latest_mv(channel_id: str, group_name: str):
    """
    指定したチャンネル内で、タイトルに
      - グループ名（スペース無視）
      - 'MV'
    が含まれる動画のみをフィルタし、
    新しい順に見て最初にヒットしたものを返す。
    
    返す dict: { "video_id": str, "title": str }
    """
    api_key = get_api_key()

    # まずは「group_name を含む動画」を新しい順で最大50件取得
    params = {
        "key": api_key,
        "part": "snippet",
        "channelId": channel_id,
        "q": group_name,      # グループ名で検索
        "type": "video",
        "order": "date",      # 新しい順
        "maxResults": 50,
    }

    resp = requests.get(YOUTUBE_SEARCH_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", [])
    if not items:
        return None

    # Python 側でタイトルフィルタ
    group_lower = group_name.lower().replace(" ", "")
    for item in items:
        title = item["snippet"]["title"]
        title_lower = title.lower()
        title_compact = title_lower.replace(" ", "")

        if group_lower in title_compact and "mv" in title_lower:
            video_id = item["id"]["videoId"]
            return {
                "video_id": video_id,
                "title": title,
            }

    # 見つからなかった場合
    return None


def get_video_stats(video_id: str):
    """
    videoId から viewCount などの統計情報を取得する。
    """
    api_key = get_api_key()

    params = {
        "key": api_key,
        "part": "statistics",
        "id": video_id,
    }

    resp = requests.get(YOUTUBE_VIDEOS_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", [])
    if not items:
        return None

    stats = items[0]["statistics"]
    view_count = int(stats.get("viewCount", 0))

    return {
        "viewCount": view_count,
        "raw": stats,
    }


def main():
    # ★必ずあなたの IVE 用の channelId に書き換えてください
    # 例: "UCxxxxxxx..."
    channel_id = "UCYDmx2Sfpnaxg488yBpZIGg"
    
    group_name = "IVE"

    latest = search_latest_mv(channel_id, group_name)
    if latest is None:
        print("MV らしき動画が見つかりませんでした。")
        return

    video_id = latest["video_id"]
    title = latest["title"]
    url = f"https://www.youtube.com/watch?v={video_id}"

    print("最新MV候補:")
    print("  title   :", title)
    print("  videoId :", video_id)
    print("  URL     :", url)

    stats = get_video_stats(video_id)
    if not stats:
        print("統計情報の取得に失敗しました。")
        return

    print()
    print("統計情報:")
    print("  viewCount :", stats["viewCount"])

    # ▼ メトリック名を動画ごとにユニークにする
    metric_name = f"kpop.youtube.viewcount.ive_{video_id}"
    value = stats["viewCount"]

    print()
    print(f"Mackerel にメトリックを投稿します... ({metric_name})")
    post_service_metric(metric_name, value)
    print("投稿完了")

if __name__ == "__main__":
    main()
