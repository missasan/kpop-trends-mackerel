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

GROUPS = [
    {
        "id": "ive",
        "name": "IVE",
        "channel_id": "UCYDmx2Sfpnaxg488yBpZIGg",
    },
    {
        "id": "aespa",
        "name": "aespa",
        "channel_id": "UCEf_Bc-KVd7onSeifS3py9g",
    },
    {
        "id": "taeyeon_panorama",
        "name": "TAEYEON Panorama",
        "channel_id": "UCEf_Bc-KVd7onSeifS3py9g",
    },
    {
        "id": "le_sserafim",
        "name": "LE SSERAFIM",
        "channel_id": "UC3IZKseVpdzPSBaWxBxundA",
    },
    {
        "id": "illit",
        "name": "ILLIT",
        "channel_id": "UC3IZKseVpdzPSBaWxBxundA",
    },
]


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


def filter_mv_items(items, group_name: str):
    """
    正式MV（末尾が MV）かつ、Remix / Performance などの派生版を除外する。
    """
    group_key = group_name.lower().replace(" ", "")

    # 除外すべきキーワード一覧（小文字）
    exclude_keywords = [
        "remix",
        "performance",
        "perf.",
        "dance",
        "choreo",
        "practice",
        "teaser",
        "highlight",
        "lyric",
        "reaction",
        "track video",
    ]

    for item in items:
        title = item["snippet"]["title"]
        title_lower = title.lower()
        title_compact = title_lower.replace(" ", "")

        # グループ名（スペースなし）がタイトルに含まれること
        if group_key not in title_compact:
            continue

        # 除外キーワードに該当したらスキップ
        if any(kw in title_lower for kw in exclude_keywords):
            continue

        # タイトル末尾が "MV"（公式MV）かどうか
        title_end = title_lower.strip()
        if not (
            title_end.endswith("mv")
            or title_end.endswith("mv)")
            or title_end.endswith("mv]")
            or title_end.endswith("official mv")
        ):
            continue

        # ここまで通れば、これは“公式MV”
        video_id = item["id"]["videoId"]
        return {"video_id": video_id, "title": title}

    return None


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

    mv = filter_mv_items(items, group_name)
    if mv:
        return mv

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
    for g in GROUPS:
        group_id = g["id"]
        group_name = g["name"]
        channel_id = g["channel_id"]

        print("=" * 60)
        print(f"[{group_id}] グループ: {group_name}")

        latest = search_latest_mv(channel_id, group_name)
        if latest is None:
            print(f"[{group_id}] MV らしき動画が見つかりませんでした。")
            continue

        video_id = latest["video_id"]
        title = latest["title"]
        url = f"https://www.youtube.com/watch?v={video_id}"

        print(f"[{group_id}] 最新MV候補:")
        print("  title   :", title)
        print("  videoId :", video_id)
        print("  URL     :", url)

        stats = get_video_stats(video_id)
        if not stats:
            print(f"[{group_id}] 統計情報の取得に失敗しました。")
            continue

        view_count = stats["viewCount"]

        print(f"[{group_id}] 統計情報:")
        print("  viewCount :", view_count)

        # メトリック名: グループID＋videoId でユニークにする
        metric_name = f"kpop.youtube.viewcount.{group_id}_{video_id}"

        print(f"[{group_id}] Mackerel にメトリックを投稿します... ({metric_name})")
        post_service_metric(metric_name, view_count)
        print(f"[{group_id}] 投稿完了")

    print("=" * 60)
    print("全グループ処理が完了しました。")

if __name__ == "__main__":
    main()
