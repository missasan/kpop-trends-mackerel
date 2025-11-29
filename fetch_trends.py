from pytrends.request import TrendReq


# ここで扱う K-POP グループを定義
GROUPS = [
    {"id": "le_sserafim", "keyword": "LE SSERAFIM"},
    {"id": "ive",         "keyword": "IVE"},
    {"id": "aespa",       "keyword": "aespa"},
    {"id": "illit",       "keyword": "ILLIT"},
]


def main():
    # pytrends のクライアントを作成
    pytrends = TrendReq(hl="ja-JP", tz=540)  # 日本向け、タイムゾーン+9時間

    # 今回使うキーワード一覧（Google Trends に渡す用）
    kw_list = [g["keyword"] for g in GROUPS]

    # 直近7日間、日本でのトレンドを取得
    pytrends.build_payload(kw_list=kw_list, timeframe="now 7-d", geo="JP")

    # 時系列データを取得
    data = pytrends.interest_over_time()

    print("=== 取得した生データ（先頭5行） ===")
    print(data.head())
    print()

    # 最新の1行を取り出す
    latest_row = data.iloc[-1]
    latest_ts = data.index[-1]

    print("=== 各グループの最新スコア ===")
    print(f"時刻: {latest_ts}")

    for g in GROUPS:
        keyword = g["keyword"]
        group_id = g["id"]

        if keyword not in latest_row:
            print(f"- {group_id}: データなし")
            continue

        value = int(latest_row[keyword])
        print(f"- {group_id} ({keyword}): {value}")


if __name__ == "__main__":
    main()
