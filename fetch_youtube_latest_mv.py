import os
import json
import requests
import isodate
import time
from datetime import datetime, timedelta
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

STATE_FILE = "state.json"
JST_OFFSET = timedelta(hours=9)
# YouTube 検索を実行する時刻（JST）
SEARCH_HOURS_JST = {14, 19}

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_youtube_api_key() -> str:
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


def calc_view_delta(state, group_id, video_id, current_view, title=None):
    """
    前回実行時との差分を返す。
    state の構造（例）:
    {
        "ive": {
            "video_id": "abc123",
            "last_view": 123456
        },
        "aespa": {
            "video_id": "def456",
            "last_view": 98765,
            "title": "some title"
        }
    }
    """

    prev_entry = state.get(group_id)
    
    # defaults
    delta = 0

    if prev_entry and prev_entry.get("video_id") == video_id:
        prev_view = prev_entry.get("last_view", 0)
        raw_delta = current_view - prev_view
        if raw_delta > 0:
            delta = raw_delta
    else:
        # MV が切り替わった or 初回 → 差分は0で開始
        delta = 0

    # state を更新
    updated_entry = {
        "video_id": video_id,
        "last_view": current_view
    }
    if title:
        updated_entry["title"] = title
    elif prev_entry and prev_entry.get("title"):
        updated_entry["title"] = prev_entry["title"]

    state[group_id] = updated_entry

    return delta


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


def get_video_duration(video_id: str):
    """動画の duration（PT3M12S など）を取得する"""
    api_key = get_youtube_api_key()
    params = {
        "key": api_key,
        "part": "contentDetails",
        "id": video_id,
    }
    resp = requests.get(YOUTUBE_VIDEOS_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", [])
    if not items:
        return None
    return items[0]["contentDetails"]["duration"]


def is_shorts(duration: str) -> bool:
    """60秒未満は Shorts と判定する"""
    try:
        td = isodate.parse_duration(duration)
        return td.total_seconds() < 60
    except Exception:
        return False


def search_latest_mv(channel_id: str, group_name: str):
    """
    指定したチャンネル内で、タイトルに
      - グループ名（スペース無視）
      - 'MV'
    が含まれる動画のみをフィルタし、
    新しい順に見て最初にヒットしたものを返す。

    さらに Shorts（60秒未満）は除外する。
    """
    api_key = get_youtube_api_key()

    # まずは「group_name を含む動画」を新しい順で最大50件取得
    params = {
        "key": api_key,
        "part": "snippet",
        "channelId": channel_id,
        "q": group_name,
        "type": "video",
        "order": "date",
        "maxResults": 50,
    }

    resp = requests.get(YOUTUBE_SEARCH_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", [])
    if not items:
        return None

    # グループ名・MV を含む候補をまず絞る
    mv = filter_mv_items(items, group_name)
    if not mv:
        return None

    # ここで Shorts 判定を入れる（duration < 60秒 → 除外）
    video_id = mv["video_id"]
    duration = get_video_duration(video_id)

    if duration and is_shorts(duration):
        # Shorts の場合は次点を探す
        group_lower = group_name.lower().replace(" ", "")

        for item in items[1:]:  # 1件目以外から再探索
            title = item["snippet"]["title"]
            title_lower = title.lower()
            title_compact = title_lower.replace(" ", "")
            if group_lower in title_compact and "mv" in title_lower:
                vid = item["id"]["videoId"]
                d2 = get_video_duration(vid)
                if d2 and not is_shorts(d2):
                    return {
                        "video_id": vid,
                        "title": title,
                    }
        return None

    # Shorts でない → 本物のMVとして返す
    return mv


def get_video_stats(video_id: str):
    """
    videoId から viewCount などの統計情報を取得する。
    """
    api_key = get_youtube_api_key()

    params = {
        "key": api_key,
        "part": "statistics,snippet",
        "id": video_id,
    }

    resp = requests.get(YOUTUBE_VIDEOS_URL, params=params, timeout=10)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        reason = None
        message = None
        body = resp.text
        try:
            data_err = resp.json()
            error_info = data_err.get("error", {})
            errors = error_info.get("errors", [])
            if errors:
                reason = errors[0].get("reason")
            message = error_info.get("message")
            body = data_err
        except Exception:
            pass

        print(
            f"[ERROR] YouTube videos.list failed (status={resp.status_code}, reason={reason}, message={message}, body={body})"
        )

        # クォータ系エラーは他グループも失敗するため知らせる
        quota_reasons = {
            "quotaExceeded",
            "dailyLimitExceeded",
            "dailyLimitExceededUnreg",
            "userRateLimitExceeded",
        }
        if reason in quota_reasons:
            return {"quota_exceeded": True}

        return None

    data = resp.json()

    items = data.get("items", [])
    if not items:
        return None

    stats = items[0]["statistics"]
    view_count = int(stats.get("viewCount", 0))
    snippet = items[0].get("snippet", {})

    return {
        "viewCount": view_count,
        "title": snippet.get("title"),
        "raw": stats,
    }


def main():
    # JST 現在時刻を取得
    current_hour_jst = (datetime.utcnow() + JST_OFFSET).hour
    should_search = current_hour_jst in SEARCH_HOURS_JST

    if should_search:
        print(f"JST {current_hour_jst}時台のため、YouTube検索を実行します。")
    else:
        print(f"JST {current_hour_jst}時台のため検索はスキップし、キャッシュ済みMVでメトリクスを送信します。")

    # ここで前回のstate.jsonを読み込む
    state = load_state()

    for g in GROUPS:
        group_id = g["id"]
        group_name = g["name"]
        channel_id = g["channel_id"]

        print("=" * 60)
        print(f"[{group_id}] グループ: {group_name}")

        latest = None
        cached_entry = state.get(group_id)

        if should_search:
            latest = search_latest_mv(channel_id, group_name)
            if latest is None:
                print(f"[{group_id}] MV らしき動画が見つかりませんでした。キャッシュがあればそれを使います。")

        if latest is None:
            if not cached_entry:
                print(f"[{group_id}] 検索を行わず、キャッシュも無いためスキップします。次の検索時間帯に更新されます。")
                continue
            video_id = cached_entry["video_id"]
            title = cached_entry.get("title", "(タイトル不明)")
            print(f"[{group_id}] キャッシュ済みのMVを使用します。")
        else:
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
        if stats.get("quota_exceeded"):
            print(f"[{group_id}] YouTube API のクォータに到達したため、残りのグループ処理を中断します。")
            break

        # タイトルがキャッシュに無い場合、動画情報から補完する
        if title == "(タイトル不明)" and stats.get("title"):
            title = stats["title"]

        view_count = stats["viewCount"]
        print(f"[{group_id}] viewCount:", view_count)

        # 絶対値メトリック
        metric_abs = f"kpop.youtube.viewcount.{group_id}_{video_id}"
        post_service_metric(metric_abs, view_count)
        print(f"[{group_id}] 絶対値メトリック投稿完了 ({metric_abs})")

        # 差分メトリック
        delta = calc_view_delta(state, group_id, video_id, view_count, title=title)
        metric_delta = f"kpop.youtube.viewdelta.{group_id}_{video_id}"
        post_service_metric(metric_delta, delta)
        print(f"[{group_id}] 差分メトリック投稿完了 ({metric_delta}): {delta}")

    # ここで state.json を保存
    save_state(state)

    print("=" * 60)
    print("全グループ処理が完了しました。")

if __name__ == "__main__":
    main()
