#!/usr/bin/env python3
"""ECサイト PoC 用シードデータ生成スクリプト。

仕様書のセグメント分布・パレート分布・季節性ルールに従い、
リアリスティックなデータを生成して postgres/init/02_seed.sql に出力する。

外部依存なし（Python 標準ライブラリのみ）。
乱数シード固定で再現性を保証する。
"""

import random
import uuid
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
RANDOM_SEED = 42
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "postgres" / "init" / "02_seed.sql"
BATCH_SIZE = 500  # 1 INSERT 文あたりの VALUES 行数

NUM_CUSTOMERS = 1_000
NUM_PRODUCTS = 80
NUM_CATEGORIES = 10
NUM_COUPONS = 30
NUM_ORDERS = 5_000
TARGET_ORDER_ITEMS = 15_000
TARGET_PAGE_VIEWS = 20_000

DATE_START = date(2024, 4, 1)
DATE_END = date(2026, 3, 31)
TOTAL_DAYS = (DATE_END - DATE_START).days + 1  # 731 days

# ---------------------------------------------------------------------------
# 日本人の姓名プール
# ---------------------------------------------------------------------------
LAST_NAMES = [
    "佐藤", "鈴木", "高橋", "田中", "伊藤", "渡辺", "山本", "中村", "小林", "加藤",
    "吉田", "山田", "佐々木", "山口", "松本", "井上", "木村", "林", "斎藤", "清水",
    "山崎", "森", "池田", "橋本", "阿部", "石川", "前田", "小川", "藤田", "岡田",
    "後藤", "長谷川", "石井", "村上", "近藤", "坂本", "遠藤", "青木", "藤井", "西村",
    "福田", "太田", "三浦", "岡本", "松田", "中島", "中野", "原田", "小野", "田村",
    "竹内", "金子", "和田", "中山", "石田", "上田", "森田", "原", "柴田", "酒井",
    "工藤", "横山", "宮崎", "宮本", "内田", "高木", "安藤", "谷口", "大野", "丸山",
    "今井", "河野", "藤原", "小松", "武田", "上野", "杉山", "千葉", "久保", "松井",
    "岩崎", "桜井", "野口", "松尾", "菅原", "市川", "野村", "新井", "渡部", "菊地",
    "佐野", "木下", "古川", "島田", "平野", "辻", "浜田", "岩田", "中田", "川口",
]

FIRST_NAMES_MALE = [
    "太郎", "一郎", "健太", "大輔", "翔太", "隆", "拓也", "直樹", "浩二", "誠",
    "亮", "大樹", "翔", "悠太", "和也", "修", "学", "秀樹", "雄太", "達也",
    "恵介", "裕太", "康介", "慎一", "智也", "大介", "勇気", "光", "海斗", "蓮",
    "颯太", "陸", "悠真", "大和", "朝陽", "結翔", "湊", "新", "樹", "律",
    "暁", "葵", "蒼", "奏", "凛", "悠", "大地", "陽翔", "碧", "遼",
]

FIRST_NAMES_FEMALE = [
    "花子", "美咲", "さくら", "陽子", "愛", "真理", "麻衣", "由美", "恵", "裕子",
    "彩", "優子", "明美", "千尋", "瞳", "舞", "遥", "綾", "凛", "葵",
    "結衣", "陽菜", "芽衣", "莉子", "心春", "紬", "美月", "詩", "杏", "楓",
    "さつき", "ひなた", "彩花", "真央", "沙織", "理恵", "香織", "友美", "恵美", "智子",
    "直美", "美穂", "早紀", "亜美", "奈々", "梨花", "桃子", "千夏", "日和", "琴音",
]

# 都道府県 (名前, 代表都市, 人口比率 ‰)
PREFECTURES = [
    ("北海道", "札幌市", 42), ("青森県", "青森市", 10), ("岩手県", "盛岡市", 10),
    ("宮城県", "仙台市", 18), ("秋田県", "秋田市", 8), ("山形県", "山形市", 9),
    ("福島県", "福島市", 15), ("茨城県", "水戸市", 23), ("栃木県", "宇都宮市", 15),
    ("群馬県", "前橋市", 15), ("埼玉県", "さいたま市", 58), ("千葉県", "千葉市", 50),
    ("東京都", "新宿区", 110), ("神奈川県", "横浜市", 73), ("新潟県", "新潟市", 18),
    ("富山県", "富山市", 8), ("石川県", "金沢市", 9), ("福井県", "福井市", 6),
    ("山梨県", "甲府市", 6), ("長野県", "長野市", 16), ("岐阜県", "岐阜市", 16),
    ("静岡県", "静岡市", 29), ("愛知県", "名古屋市", 60), ("三重県", "津市", 14),
    ("滋賀県", "大津市", 11), ("京都府", "京都市", 20), ("大阪府", "大阪市", 70),
    ("兵庫県", "神戸市", 43), ("奈良県", "奈良市", 10), ("和歌山県", "和歌山市", 7),
    ("鳥取県", "鳥取市", 5), ("島根県", "松江市", 5), ("岡山県", "岡山市", 15),
    ("広島県", "広島市", 22), ("山口県", "山口市", 11), ("徳島県", "徳島市", 6),
    ("香川県", "高松市", 8), ("愛媛県", "松山市", 11), ("高知県", "高知市", 6),
    ("福岡県", "福岡市", 41), ("佐賀県", "佐賀市", 6), ("長崎県", "長崎市", 11),
    ("熊本県", "熊本市", 14), ("大分県", "大分市", 9), ("宮崎県", "宮崎市", 9),
    ("鹿児島県", "鹿児島市", 13), ("沖縄県", "那覇市", 12),
]

PREF_NAMES = [p[0] for p in PREFECTURES]
PREF_CITIES = [p[1] for p in PREFECTURES]
PREF_WEIGHTS = [p[2] for p in PREFECTURES]

# カテゴリ定義 (id, name, parent_id or None)
CATEGORIES = [
    (1, "電化製品", None),
    (2, "衣料品", None),
    (3, "食品・飲料", None),
    (4, "書籍", None),
    (5, "スポーツ・アウトドア", None),
    (6, "スマートフォン・周辺機器", 1),
    (7, "メンズファッション", 2),
    (8, "スイーツ・菓子", 3),
    (9, "コンピュータ・IT", 4),
    (10, "キャンプ用品", 5),
]

# カテゴリの季節性 (category_id -> {month: multiplier})
# 基準1.0, 繁忙期は1.3-1.5, 閑散期は0.6-0.8
# 子カテゴリは親の季節性を継承
_PARENT_SEASONALITY = {
    1: {3: 1.5, 12: 1.5, 2: 0.7, 8: 0.7},           # 電化製品
    2: {4: 1.4, 10: 1.4, 12: 1.3, 1: 0.7, 7: 0.7},   # 衣料品
    3: {2: 1.4, 12: 1.5, 3: 1.3, 6: 0.7, 9: 0.7},    # 食品
    4: {4: 1.3, 9: 1.3, 8: 0.7},                       # 書籍
    5: {5: 1.4, 7: 1.3, 8: 1.3, 1: 0.7, 2: 0.7},     # スポーツ
}
CATEGORY_SEASONALITY = {**_PARENT_SEASONALITY}
for cat_id, _, parent_id in CATEGORIES:
    if parent_id is not None and parent_id in _PARENT_SEASONALITY:
        CATEGORY_SEASONALITY[cat_id] = _PARENT_SEASONALITY[parent_id]

# 商品定義
PRODUCTS_DEF = [
    # (product_id, name, category_id, unit_price, description, cost_price, weight_gram, is_active, popularity_rank)
    # 電化製品 (cat 1)
    (1, "ワイヤレスイヤホン Pro", 1, 12800, "ノイズキャンセリング搭載", 5120, 45, True, "A"),
    (2, "4K液晶テレビ 55インチ", 1, 89800, "HDR対応高画質モデル", 44900, 18000, True, "A"),
    (3, "ロボット掃除機 X100", 1, 49800, "AI搭載自動走行モデル", 19920, 3500, True, "A"),
    (4, "電子レンジ オーブン付き", 1, 32800, "コンベクション機能搭載", 13120, 12000, True, "B"),
    (5, "空気清浄機 プラズマ", 1, 28800, "花粉・PM2.5対応", 11520, 6500, True, "B"),
    (6, "ドライヤー ナノケア", 1, 18800, "マイナスイオン搭載", 7520, 580, True, "B"),
    (7, "電気ケトル 1.2L", 1, 5800, "保温機能付き", 2320, 1200, True, "C"),
    (8, "LEDデスクライト", 1, 6800, "調光調色機能付き", 2720, 900, True, "C"),
    # スマホ・周辺機器 (cat 6)
    (9, "スマートフォン Galaxy S24", 6, 124800, "最新フラッグシップ", 74880, 195, True, "A"),
    (10, "スマートフォン iPhone 15", 6, 134800, "A17チップ搭載", 80880, 187, True, "A"),
    (11, "ワイヤレス充電器", 6, 3980, "Qi対応急速充電", 1592, 120, True, "B"),
    (12, "スマホケース 手帳型", 6, 2480, "本革仕様", 744, 80, True, "C"),
    (13, "USB-Cケーブル 2m", 6, 1280, "急速充電対応", 384, 45, True, "C"),
    (14, "モバイルバッテリー 20000mAh", 6, 4980, "PD急速充電対応", 1992, 350, True, "B"),
    # 衣料品 (cat 2)
    (15, "メンズ ダウンジャケット", 2, 19800, "防寒性抜群の冬物", 7920, 650, True, "A"),
    (16, "レディース ワンピース", 2, 8800, "フローラル柄春物", 3520, 280, True, "A"),
    (17, "ユニセックス パーカー", 2, 5800, "オーガニックコットン", 2320, 450, True, "B"),
    (18, "メンズ チノパン", 2, 6800, "ストレッチ素材", 2720, 380, True, "B"),
    (19, "レディース スカート", 2, 4800, "プリーツデザイン", 1920, 220, True, "C"),
    (20, "キッズ Tシャツ", 2, 1980, "キャラクタープリント", 792, 120, True, "C"),
    # メンズファッション (cat 7)
    (21, "ビジネスシューズ 本革", 7, 15800, "イタリアンレザー", 6320, 780, True, "B"),
    (22, "ネクタイ シルク", 7, 4800, "ストライプ柄", 1440, 80, True, "C"),
    (23, "メンズ 腕時計", 7, 29800, "ソーラー電波", 11920, 120, True, "A"),
    (24, "カジュアルジャケット", 7, 12800, "テーラードフィット", 5120, 500, True, "B"),
    # 食品・飲料 (cat 3)
    (25, "有機栽培コーヒー豆 500g", 3, 2480, "フェアトレード認証", 992, 520, True, "A"),
    (26, "国産はちみつ 300g", 3, 1980, "非加熱・純粋", 792, 350, True, "B"),
    (27, "オリーブオイル EXV 500ml", 3, 1680, "イタリア産", 672, 580, True, "B"),
    (28, "黒酢ドリンク 900ml", 3, 1280, "りんご果汁入り", 512, 950, True, "C"),
    (29, "ミックスナッツ 400g", 3, 1480, "無塩ロースト", 592, 420, True, "A"),
    (30, "抹茶ラテ粉末 200g", 3, 980, "京都産宇治抹茶使用", 392, 220, True, "C"),
    # スイーツ (cat 8)
    (31, "チョコレートアソート 24個", 8, 3280, "ベルギー産カカオ", 1312, 400, True, "A"),
    (32, "マカロン詰め合わせ 12個", 8, 2980, "パティシエ手作り", 1192, 300, True, "B"),
    (33, "バウムクーヘン", 8, 1680, "しっとり焼き上げ", 672, 500, True, "B"),
    (34, "和菓子セット 10個", 8, 2480, "季節の上生菓子", 992, 450, True, "C"),
    (35, "プリン 6個セット", 8, 1980, "濃厚カスタード", 792, 600, True, "C"),
    # 書籍 (cat 4)
    (36, "Python入門 第3版", 4, 2980, "初心者向けプログラミング", 1192, 500, True, "A"),
    (37, "データ分析の教科書", 4, 3280, "実践的なデータサイエンス", 1312, 450, True, "A"),
    (38, "英語学習ガイド", 4, 1680, "TOEIC対策", 672, 350, True, "B"),
    (39, "ビジネス書 リーダーシップ論", 4, 1580, "ベストセラー", 632, 300, True, "B"),
    (40, "小説 夜の向こう", 4, 1480, "直木賞受賞作", 592, 280, True, "C"),
    # コンピュータ・IT (cat 9)
    (41, "機械学習実践ガイド", 9, 3980, "TensorFlow & PyTorch", 1592, 550, True, "A"),
    (42, "Webアプリ開発入門", 9, 2780, "React + Node.js", 1112, 400, True, "B"),
    (43, "データベース設計の基礎", 9, 3180, "正規化からNoSQLまで", 1272, 480, True, "B"),
    (44, "クラウドアーキテクチャ", 9, 3480, "AWS/GCP/Azure比較", 1392, 500, True, "C"),
    (45, "セキュリティ入門", 9, 2680, "ゼロトラスト時代の", 1072, 380, True, "C"),
    # スポーツ (cat 5)
    (46, "ランニングシューズ", 5, 12800, "軽量クッション", 5120, 260, True, "A"),
    (47, "ヨガマット 6mm", 5, 3980, "TPE素材エコ", 1592, 1200, True, "B"),
    (48, "プロテイン ホエイ 1kg", 5, 3480, "チョコレート風味", 1392, 1050, True, "A"),
    (49, "ダンベルセット 20kg", 5, 8980, "可変式ラバーコート", 3592, 20500, True, "B"),
    (50, "スポーツタオル 3枚セット", 5, 1980, "速乾マイクロファイバー", 792, 300, True, "C"),
    # キャンプ用品 (cat 10)
    (51, "テント 2人用", 10, 19800, "ダブルウォール自立式", 7920, 2800, True, "A"),
    (52, "寝袋 マミー型", 10, 8980, "快適温度0度", 3592, 1500, True, "B"),
    (53, "LEDランタン", 10, 3980, "USB充電式1000ルーメン", 1592, 350, True, "B"),
    (54, "キャンプチェア", 10, 4980, "ハイバック折りたたみ", 1992, 2500, True, "C"),
    (55, "クッカーセット", 10, 5980, "アルミ軽量7点", 2392, 800, True, "C"),
    # --- 新規追加30商品 ---
    (56, "Bluetoothスピーカー", 1, 7980, "防水IPX7対応", 3192, 380, True, "B"),
    (57, "電動歯ブラシ", 1, 9800, "音波振動式", 3920, 200, True, "C"),
    (58, "ポータブルプロジェクター", 1, 39800, "フルHD対応", 15920, 900, True, "B"),
    (59, "スマートウォッチ", 6, 29800, "健康管理機能搭載", 11920, 48, True, "A"),
    (60, "タブレット 10インチ", 6, 49800, "高解像度ディスプレイ", 19920, 460, True, "A"),
    (61, "ワイヤレスキーボード", 6, 6980, "薄型Bluetooth", 2792, 380, True, "C"),
    (62, "レディース コート", 2, 24800, "ウール混カシミア", 9920, 1200, True, "B"),
    (63, "メンズ スニーカー", 2, 8980, "レトロデザイン", 3592, 650, True, "B"),
    (64, "レディース バッグ", 2, 14800, "2WAYショルダー", 5920, 480, True, "A"),
    (65, "ベルト 本革", 7, 3980, "ビジネスカジュアル", 1592, 150, True, "C"),
    (66, "カフスボタン セット", 7, 5800, "シルバー925", 2320, 40, True, "C"),
    (67, "緑茶 ティーバッグ 50P", 3, 1280, "静岡産一番茶", 512, 200, True, "B"),
    (68, "グラノーラ 500g", 3, 880, "フルーツミックス", 352, 520, True, "C"),
    (69, "フルーツジャム 3種セット", 8, 1580, "国産果実使用", 632, 750, True, "C"),
    (70, "ドライフルーツミックス 300g", 8, 1280, "無添加オーガニック", 512, 320, True, "C"),
    (71, "AI時代の働き方", 4, 1780, "キャリア戦略", 712, 280, True, "B"),
    (72, "宇宙の謎に迫る", 4, 1980, "最新天文学入門", 792, 320, True, "C"),
    (73, "Kubernetes実践入門", 9, 3680, "コンテナオーケストレーション", 1472, 520, True, "B"),
    (74, "サイバーセキュリティ白書", 9, 2980, "最新脅威レポート", 1192, 450, True, "C"),
    (75, "トレイルランシューズ", 5, 14800, "グリップ重視", 5920, 300, True, "B"),
    (76, "フォームローラー", 5, 2980, "筋膜リリース用", 1192, 800, True, "C"),
    (77, "焚き火台", 10, 7980, "ステンレス折りたたみ", 3192, 1800, True, "B"),
    (78, "ハンモック", 10, 4980, "パラシュート生地", 1992, 600, True, "C"),
    (79, "防災ラジオ", 1, 4980, "手回し充電ソーラー", 1992, 300, True, "C"),
    (80, "加湿器 超音波式", 1, 6980, "大容量4L", 2792, 1800, False, "C"),
]

# Popularity weight by rank
POPULARITY_WEIGHTS = {"A": 10.0, "B": 4.0, "C": 1.5}


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------
def weighted_choice(rng, options, weights):
    """重み付きランダム選択（random.choices相当だが明示的に）"""
    return rng.choices(options, weights=weights, k=1)[0]


def sql_str(val):
    """値をSQL用文字列に変換する"""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, date) and not isinstance(val, datetime):
        return f"'{val.isoformat()}'"
    if isinstance(val, datetime):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
    # String: escape single quotes
    s = str(val).replace("'", "''")
    return f"'{s}'"


def write_batch_inserts(out, table_name, columns, rows, batch_size=BATCH_SIZE):
    """バッチ INSERT 文を出力する"""
    if not rows:
        return
    cols_str = ", ".join(columns)
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        out.write(f"INSERT INTO {table_name} ({cols_str}) VALUES\n")
        for j, row in enumerate(batch):
            vals = ", ".join(sql_str(v) for v in row)
            sep = "," if j < len(batch) - 1 else ";"
            out.write(f"({vals}){sep}\n")
    out.write("\n")


# ---------------------------------------------------------------------------
# データ生成関数
# ---------------------------------------------------------------------------
def generate_customers(rng):
    """顧客 10,000 人を生成"""
    # セグメント分布 (比率: heavy 5%, middle 15%, light 30%, dormant 20%, new 20%, churn 10%)
    segments = []
    for seg, pct in [("heavy", 5), ("middle", 15), ("light", 30),
                     ("dormant", 20), ("new", 20), ("churn", 10)]:
        segments.extend([seg] * (NUM_CUSTOMERS * pct // 100))
    while len(segments) < NUM_CUSTOMERS:
        segments.append("light")
    rng.shuffle(segments)

    # 性別分布
    genders = ["男性"] * (NUM_CUSTOMERS * 48 // 100) + ["女性"] * (NUM_CUSTOMERS * 50 // 100) + ["その他"] * (NUM_CUSTOMERS * 2 // 100)
    while len(genders) < NUM_CUSTOMERS:
        genders.append("女性")
    rng.shuffle(genders)

    # 年代分布 -> birth_date
    age_groups = (["20s"] * (NUM_CUSTOMERS * 15 // 100) + ["30s"] * (NUM_CUSTOMERS * 30 // 100) +
                  ["40s"] * (NUM_CUSTOMERS * 25 // 100) + ["50s"] * (NUM_CUSTOMERS * 20 // 100) +
                  ["60s"] * (NUM_CUSTOMERS * 10 // 100))
    while len(age_groups) < NUM_CUSTOMERS:
        age_groups.append("30s")
    rng.shuffle(age_groups)

    # 登録チャネル分布
    channels = (["organic"] * (NUM_CUSTOMERS * 40 // 100) + ["ad_search"] * (NUM_CUSTOMERS * 25 // 100) +
                ["ad_social"] * (NUM_CUSTOMERS * 20 // 100) + ["referral"] * (NUM_CUSTOMERS * 10 // 100) +
                ["affiliate"] * (NUM_CUSTOMERS * 5 // 100))
    while len(channels) < NUM_CUSTOMERS:
        channels.append("organic")
    rng.shuffle(channels)

    customers = []

    for i in range(NUM_CUSTOMERS):
        cid = i + 1
        gender = genders[i]
        segment = segments[i]

        if gender == "男性":
            first_name = rng.choice(FIRST_NAMES_MALE)
        elif gender == "女性":
            first_name = rng.choice(FIRST_NAMES_FEMALE)
        else:
            first_name = rng.choice(FIRST_NAMES_MALE + FIRST_NAMES_FEMALE)

        last_name = rng.choice(LAST_NAMES)

        email_base = f"user{cid:05d}@example.com"

        phone = f"090-{rng.randint(1000, 9999):04d}-{rng.randint(1000, 9999):04d}"

        pref_idx = weighted_choice(rng, list(range(len(PREFECTURES))), PREF_WEIGHTS)
        prefecture = PREF_NAMES[pref_idx]
        city = PREF_CITIES[pref_idx]
        postal_code = f"{rng.randint(100, 999):03d}-{rng.randint(0, 9999):04d}"

        # 登録日: セグメントに依存
        if segment == "new":
            # 直近3ヶ月
            reg_date = DATE_END - timedelta(days=rng.randint(1, 90))
        elif segment == "churn":
            # 初期に登録
            reg_date = DATE_START + timedelta(days=rng.randint(0, 180))
        else:
            # 2年間に分散
            reg_date = DATE_START + timedelta(days=rng.randint(0, TOTAL_DAYS - 1))

        created_at = datetime(reg_date.year, reg_date.month, reg_date.day,
                              rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))

        # 年代 -> birth_date
        age_group = age_groups[i]
        ref_date = date(2025, 4, 1)
        if age_group == "20s":
            age = rng.randint(20, 29)
        elif age_group == "30s":
            age = rng.randint(30, 39)
        elif age_group == "40s":
            age = rng.randint(40, 49)
        elif age_group == "50s":
            age = rng.randint(50, 59)
        else:
            age = rng.randint(60, 75)
        birth_year = ref_date.year - age
        birth_date = date(birth_year, rng.randint(1, 12), rng.randint(1, 28))

        channel = channels[i]

        customers.append((
            cid, first_name, last_name, email_base, phone, city, prefecture,
            postal_code, created_at, gender, birth_date, channel, segment
        ))

    return customers


def generate_categories():
    """カテゴリ 10 件"""
    return CATEGORIES


def generate_products():
    """商品 80 件"""
    return PRODUCTS_DEF


def generate_coupons(rng):
    """クーポン 30 件"""
    coupons = []
    cid = 0

    # 季節セール 10件
    seasonal = [
        ("NEWYEAR2025", "percentage", 15.00, 3000, date(2024, 12, 26), date(2025, 1, 10), "新年セール2025"),
        ("GW2025", "percentage", 10.00, 2000, date(2025, 4, 26), date(2025, 5, 6), "GWセール2025"),
        ("SUMMER2025", "percentage", 20.00, 5000, date(2025, 7, 20), date(2025, 8, 20), "夏のビッグセール2025"),
        ("AUTUMN2025", "fixed_amount", 500.00, 3000, date(2025, 9, 15), date(2025, 10, 15), "秋の感謝セール2025"),
        ("BLACKFRI2025", "percentage", 25.00, 5000, date(2025, 11, 22), date(2025, 11, 30), "ブラックフライデー2025"),
        ("XMAS2025", "percentage", 15.00, 3000, date(2025, 12, 1), date(2025, 12, 25), "クリスマスセール2025"),
        ("NEWYEAR2026", "percentage", 15.00, 3000, date(2025, 12, 26), date(2026, 1, 10), "新年セール2026"),
        ("SPRING2025", "fixed_amount", 300.00, 2000, date(2025, 3, 1), date(2025, 3, 31), "春の新生活セール"),
        ("OBON2025", "percentage", 10.00, 2000, date(2025, 8, 10), date(2025, 8, 16), "お盆セール2025"),
        ("YEAREND2024", "percentage", 20.00, 5000, date(2024, 12, 1), date(2024, 12, 31), "年末セール2024"),
    ]
    for code, ctype, val, min_amt, vf, vt, name in seasonal:
        cid += 1
        coupons.append((cid, code, ctype, val, min_amt, vf, vt, name))

    # 新規顧客向け 5件
    for j in range(5):
        cid += 1
        code = f"WELCOME{j+1:02d}"
        coupons.append((cid, code, "percentage", 10.00 + j * 2, 1000,
                         DATE_START, DATE_END, f"新規顧客歓迎クーポン{j+1}"))

    # リピーター向け 5件
    for j in range(5):
        cid += 1
        code = f"REPEAT{j+1:02d}"
        coupons.append((cid, code, "fixed_amount", 300.00 + j * 100, 3000 + j * 500,
                         DATE_START, DATE_END, f"リピーター感謝クーポン{j+1}"))

    # カテゴリ限定 5件
    cat_names = ["電化製品", "衣料品", "食品", "書籍", "スポーツ"]
    for j in range(5):
        cid += 1
        code = f"CAT{j+1:02d}"
        coupons.append((cid, code, "percentage", 10.00 + j * 3, 2000,
                         date(2025, 1, 1), date(2025, 12, 31), f"{cat_names[j]}限定クーポン"))

    # フラッシュセール 5件
    for j in range(5):
        cid += 1
        start = date(2025, 1 + j * 2, 15)
        code = f"FLASH{j+1:02d}"
        coupons.append((cid, code, "percentage", 30.00 + j * 5, 5000,
                         start, start + timedelta(days=2), f"フラッシュセール{j+1}"))

    return coupons


def generate_orders_and_items(rng, customers, products, coupons):
    """注文 50,000 件と注文明細 ~150,000 件を生成"""
    # Prepare product lookup
    product_ids = [p[0] for p in products]
    product_prices = {p[0]: p[3] for p in products}
    product_weights_pop = [POPULARITY_WEIGHTS[p[8]] for p in products]

    # Prepare coupon codes
    coupon_codes = [c[1] for c in coupons]

    # Segment -> customer mapping
    seg_customers = {}
    for c in customers:
        seg = c[12]  # segment field
        if seg not in seg_customers:
            seg_customers[seg] = []
        seg_customers[seg].append(c[0])  # customer_id

    # Customer registration dates
    customer_reg_dates = {c[0]: c[8].date() if isinstance(c[8], datetime) else c[8] for c in customers}
    customer_segments = {c[0]: c[12] for c in customers}

    # 時間帯重み (0-23)
    hour_weights = [0.05/6]*6 + [0.10/3]*3 + [0.15/3]*3 + [0.20/2]*2 + [0.15/4]*4 + [0.25/3]*3 + [0.10/3]*3
    # Normalize (they should already be ~1.0 but let's be safe)
    hw_sum = sum(hour_weights)
    hour_weights = [w/hw_sum for w in hour_weights]

    # セグメント別注文数割り当て
    # heavy: 月2-4回 = 24年で48-96回 -> avg 72
    # middle: 月0.5-1回 = 12-24回 -> avg 18
    # light: 3-6ヶ月に1回 = 4-8回 -> avg 6
    # dormant: 1-2回
    # new: 1-2回
    # churn: 0 (離脱 = 登録のみで未購入またはキャンセルのみ)

    # We need to allocate exactly 50,000 orders across segments
    # First calculate rough counts per customer in each segment
    seg_order_ranges = {
        "heavy": (48, 96),
        "middle": (12, 24),
        "light": (4, 8),
        "dormant": (1, 3),
        "new": (1, 3),
        "churn": (0, 1),  # 0 or 1 (cancelled)
    }

    # Assign orders per customer
    customer_order_counts = {}
    total_allocated = 0

    for seg in ["heavy", "middle", "light", "dormant", "new", "churn"]:
        cids = seg_customers.get(seg, [])
        lo, hi = seg_order_ranges[seg]
        for cid in cids:
            cnt = rng.randint(lo, hi)
            customer_order_counts[cid] = cnt
            total_allocated += cnt

    # Adjust to hit exactly NUM_ORDERS
    # Scale proportionally
    if total_allocated != NUM_ORDERS:
        scale = NUM_ORDERS / total_allocated
        total_allocated = 0
        for cid in customer_order_counts:
            customer_order_counts[cid] = max(1 if customer_segments[cid] != "churn" else 0,
                                              round(customer_order_counts[cid] * scale))
            total_allocated += customer_order_counts[cid]

        # Fine-tune: adjust random heavy/middle customers
        diff = NUM_ORDERS - total_allocated
        adjustable = [cid for cid in customer_order_counts
                      if customer_segments[cid] in ("heavy", "middle")]
        rng.shuffle(adjustable)
        idx = 0
        while diff != 0:
            cid = adjustable[idx % len(adjustable)]
            if diff > 0:
                customer_order_counts[cid] += 1
                diff -= 1
            elif diff < 0 and customer_order_counts[cid] > 1:
                customer_order_counts[cid] -= 1
                diff += 1
            idx += 1
            if idx > len(adjustable) * 10:
                break

    # ステータス分布
    # delivered 55%, shipped 12%, confirmed 10%, pending 8%, cancelled 15%
    status_dist = ["delivered"] * 55 + ["shipped"] * 12 + ["confirmed"] * 10 + ["pending"] * 8 + ["cancelled"] * 15

    # 決済方法分布
    payment_base = ["credit_card"] * 55 + ["bank_transfer"] * 15 + ["convenience_store"] * 15 + ["carrier"] * 10 + ["cod"] * 5

    # デバイス分布
    device_base = ["mobile"] * 60 + ["desktop"] * 30 + ["tablet"] * 10

    # 注文明細数分布 (target avg ~3 items per order to reach 150k)
    item_count_dist = [1] * 20 + [2] * 25 + [3] * 25 + [4] * 15 + [5] * 8 + [6] * 4 + [7] * 3

    # 数量分布
    qty_dist = [1] * 70 + [2] * 15 + [3] * 8 + [4] * 4 + [5] * 3

    # 割引率分布
    discount_dist = [0.00] * 60 + [5.00] * 15 + [10.00] * 15 + [15.00] * 5 + [20.00] * 5

    orders = []
    order_items = []
    order_id = 0
    order_item_id = 0

    # Precompute seasonality-adjusted product weights per month
    seasonal_weights_by_month = {}
    for m in range(1, 13):
        weights = []
        for p in products:
            base_w = POPULARITY_WEIGHTS[p[8]]
            mult = CATEGORY_SEASONALITY.get(p[2], {}).get(m, 1.0)
            weights.append(base_w * mult)
        seasonal_weights_by_month[m] = weights

    # Segment-specific distributions (precomputed once)
    status_new = ["delivered"] * 30 + ["shipped"] * 15 + ["confirmed"] * 20 + ["pending"] * 25 + ["cancelled"] * 10
    status_recent = ["pending"] * 30 + ["confirmed"] * 25 + ["shipped"] * 20 + ["delivered"] * 15 + ["cancelled"] * 10
    payment_heavy = ["credit_card"] * 75 + ["bank_transfer"] * 10 + ["convenience_store"] * 5 + ["carrier"] * 7 + ["cod"] * 3
    payment_light = ["credit_card"] * 40 + ["bank_transfer"] * 15 + ["convenience_store"] * 25 + ["carrier"] * 12 + ["cod"] * 8
    hours_list = list(range(24))

    def _pick_order_items(n_items, weights):
        nonlocal order_item_id
        chosen = set()
        for _ in range(n_items):
            pid = weighted_choice(rng, product_ids, weights)
            while pid in chosen and len(chosen) < len(product_ids):
                pid = weighted_choice(rng, product_ids, weights)
            chosen.add(pid)
            order_item_id += 1
            qty = rng.choice(qty_dist)
            order_items.append((order_item_id, order_id, pid, qty, product_prices[pid], rng.choice(discount_dist)))

    for cid, num_orders in sorted(customer_order_counts.items()):
        if num_orders == 0:
            continue

        reg_date = customer_reg_dates[cid]
        seg = customer_segments[cid]

        # Determine valid date range for this customer
        valid_start = max(DATE_START, reg_date)
        if valid_start > DATE_END:
            valid_start = DATE_END - timedelta(days=1)
        valid_days = (DATE_END - valid_start).days + 1
        if valid_days < 1:
            valid_days = 1

        if seg == "dormant":
            dormant_end = min(valid_start + timedelta(days=180), DATE_END)
            valid_days_dormant = (dormant_end - valid_start).days + 1
        else:
            valid_days_dormant = valid_days

        for _ in range(num_orders):
            order_id += 1

            # Pick date based on segment
            if seg == "new":
                new_start = max(valid_start, DATE_END - timedelta(days=90))
                day_offset = rng.randint(0, max(0, (DATE_END - new_start).days))
                order_date = new_start + timedelta(days=day_offset)
            elif seg == "dormant":
                day_offset = rng.randint(0, max(0, valid_days_dormant - 1))
                order_date = valid_start + timedelta(days=day_offset)
            else:
                day_offset = rng.randint(0, max(0, valid_days - 1))
                order_date = valid_start + timedelta(days=day_offset)

            hour = weighted_choice(rng, hours_list, hour_weights)
            order_timestamp = datetime(order_date.year, order_date.month, order_date.day,
                                       hour, rng.randint(0, 59), rng.randint(0, 59))

            # Status
            days_ago = (DATE_END - order_date).days
            if seg == "new":
                status = rng.choice(status_new)
            elif days_ago < 14:
                status = rng.choice(status_recent)
            elif seg == "churn":
                status = "cancelled"
            else:
                status = rng.choice(status_dist)

            # Payment
            if seg == "heavy":
                payment = rng.choice(payment_heavy)
            elif seg == "light":
                payment = rng.choice(payment_light)
            else:
                payment = rng.choice(payment_base)

            device = rng.choice(device_base)
            coupon = rng.choice(coupon_codes) if rng.random() < 0.15 else None
            shipping_addr = f"{PREF_NAMES[rng.randint(0, len(PREF_NAMES)-1)]}某所"

            orders.append((
                order_id, cid, order_date, shipping_addr, status,
                order_timestamp, order_timestamp,
                order_timestamp, payment, device, coupon
            ))

            n_items = rng.choice(item_count_dist)
            weights = product_weights_pop if seg == "new" else seasonal_weights_by_month[order_date.month]
            _pick_order_items(n_items, weights)

    return orders, order_items


def generate_page_views(rng, customers, orders, products):
    """ページビュー ~500,000 件を生成。

    1) 購入済み注文に対して必ず product->cart->checkout->complete のセッションを逆算生成
    2) ランダムセッション（ファネル離脱を含む）で残り件数を埋める
    """
    page_views = []
    pv_id = 0

    product_ids = [p[0] for p in products]
    product_categories = {p[0]: p[2] for p in products}

    referrer_dist = (["direct"] * 30 + ["search_organic"] * 25 + ["search_paid"] * 15 +
                     ["social"] * 15 + ["email"] * 10 + ["referral"] * 5)

    page_types_funnel = ["top", "category", "product", "cart", "checkout", "complete"]

    # カテゴリ -> 商品ID マッピング
    cat_products = {}
    for p in products:
        cat_id = p[2]
        if cat_id not in cat_products:
            cat_products[cat_id] = []
        cat_products[cat_id].append(p[0])

    # 1) 購入注文からセッション生成 (non-cancelled orders)
    purchase_orders = [(o[0], o[1], o[7]) for o in orders if o[4] != "cancelled"]
    # o[0]=order_id, o[1]=customer_id, o[7]=order_timestamp

    # order_id -> order_items
    order_items_map = {}
    # We'll need order_items passed - but we don't have them here
    # Instead, just generate purchase funnel PVs based on order timestamp

    for order_id, cust_id, order_ts in purchase_orders:
        if isinstance(order_ts, datetime):
            base_ts = order_ts
        else:
            base_ts = datetime(order_ts.year, order_ts.month, order_ts.day, 12, 0, 0)

        session_id = f"s-{uuid.UUID(int=rng.getrandbits(128)).hex[:12]}"
        referrer = rng.choice(referrer_dist)

        # Generate: top -> category -> product -> cart -> checkout -> complete
        # Each step is 30-180 seconds apart
        ts = base_ts - timedelta(seconds=rng.randint(300, 900))  # Start 5-15 min before order

        # Pick a product from order (just pick random product)
        prod_id = rng.choice(product_ids)
        cat_id = product_categories.get(prod_id, 1)

        steps = [
            ("top", None, None),
            ("category", None, cat_id),
            ("product", prod_id, cat_id),
            ("cart", None, None),
            ("checkout", None, None),
            ("complete", None, None),
        ]

        for page_type, pid, cid_page in steps:
            pv_id += 1
            duration = rng.randint(10, 120)
            page_views.append((
                pv_id, cust_id, session_id, page_type,
                pid, cid_page, referrer,
                ts, duration
            ))
            ts = ts + timedelta(seconds=duration + rng.randint(5, 30))

    # How many PVs so far
    purchase_pvs = len(page_views)
    remaining = TARGET_PAGE_VIEWS - purchase_pvs
    if remaining < 0:
        remaining = 0

    # 2) ランダムセッション生成
    # ログインユーザー 70%, 未ログイン 30%
    customer_ids = [c[0] for c in customers]

    session_avg_pages = 5  # average pages per session (conservative estimate)
    num_sessions = int(remaining / session_avg_pages * 1.05)  # slightly over-provision

    for _ in range(num_sessions):
        # Logged in or anonymous
        if rng.random() < 0.70:
            cust_id = rng.choice(customer_ids)
        else:
            cust_id = None

        session_id = f"s-{uuid.UUID(int=rng.getrandbits(128)).hex[:12]}"
        referrer = rng.choice(referrer_dist)

        # Random date/time
        day_offset = rng.randint(0, TOTAL_DAYS - 1)
        session_date = DATE_START + timedelta(days=day_offset)
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        ts = datetime(session_date.year, session_date.month, session_date.day, hour, minute, rng.randint(0, 59))

        # Funnel simulation
        # Start with top or search
        start_page = rng.choice(["top", "search"])
        cat_id = rng.choice([c[0] for c in CATEGORIES])
        prod_id = rng.choice(cat_products.get(cat_id, product_ids))

        funnel = [start_page]
        # top/search -> category (60%)
        if rng.random() < 0.60:
            funnel.append("category")
            # category -> product (40%)
            if rng.random() < 0.40:
                funnel.append("product")
                # product -> cart (15%)
                if rng.random() < 0.15:
                    funnel.append("cart")
                    # cart -> checkout (60%)
                    if rng.random() < 0.60:
                        funnel.append("checkout")
                        # checkout -> complete (85%)
                        if rng.random() < 0.85:
                            funnel.append("complete")

        # Add some random browsing pages (mypage, search, additional products)
        extra_pages = rng.randint(1, 5)
        for _ in range(extra_pages):
            funnel.append(rng.choice(["product", "category", "search", "mypage"]))

        for page_type in funnel:
            pv_id += 1
            if page_type == "product":
                pid = prod_id
                cid_page = product_categories.get(prod_id)
            elif page_type == "category":
                pid = None
                cid_page = cat_id
            else:
                pid = None
                cid_page = None

            duration = rng.randint(5, 180)
            page_views.append((
                pv_id, cust_id, session_id, page_type,
                pid, cid_page, referrer,
                ts, duration
            ))
            ts = ts + timedelta(seconds=duration + rng.randint(2, 20))

            if len(page_views) >= TARGET_PAGE_VIEWS:
                break

        if len(page_views) >= TARGET_PAGE_VIEWS:
            break

    return page_views


def generate_inventory_snapshots(rng, products, orders, order_items):
    """日次在庫スナップショット ~27,000 件を生成。

    初期在庫から日次販売数を減算し、発注点到達時に補充。
    一部商品で意図的に欠品期間を作る。
    """
    product_ids = [p[0] for p in products]
    product_ranks = {p[0]: p[8] for p in products}
    is_active = {p[0]: p[7] for p in products}

    # 日別・商品別の販売数を集計
    order_dates = {o[0]: o[2] for o in orders}
    daily_sales = {}  # (product_id, date) -> total_qty
    for item in order_items:
        oi_order_id = item[1]
        pid = item[2]
        qty = item[3]
        odate = order_dates.get(oi_order_id)
        if odate:
            key = (pid, odate)
            daily_sales[key] = daily_sales.get(key, 0) + qty

    snapshots = []
    snap_id = 0

    # 欠品対象商品（ランダムに5商品選択）
    stockout_products = set(rng.sample(product_ids, 5))
    stockout_periods = {}
    for pid in stockout_products:
        # ランダムな期間に欠品
        start_day = rng.randint(100, 500)
        duration = rng.randint(7, 30)
        stockout_periods[pid] = (start_day, start_day + duration)

    # Track subset of products for subset of days to stay within Emulator limits
    tracked_products = rng.sample(products, min(20, len(products)))
    tracking_days = min(365, TOTAL_DAYS)

    for p in tracked_products:
        pid = p[0]
        rank = p[8]

        # 初期在庫
        if rank == "A":
            initial_stock = rng.randint(200, 500)
            reorder_point = rng.randint(30, 50)
        elif rank == "B":
            initial_stock = rng.randint(100, 200)
            reorder_point = rng.randint(20, 30)
        else:
            initial_stock = rng.randint(20, 80)
            reorder_point = rng.randint(10, 20)

        lead_time = rng.randint(3, 14)
        stock = initial_stock
        reserved = 0
        pending_reorder = None  # (arrival_day_offset, quantity)

        for day_offset in range(tracking_days):
            current_date = DATE_START + timedelta(days=day_offset)

            # 欠品期間中は補充しない
            is_stockout_period = False
            if pid in stockout_periods:
                so_start, so_end = stockout_periods[pid]
                if so_start <= day_offset <= so_end:
                    is_stockout_period = True

            # 補充到着チェック
            if pending_reorder and day_offset >= pending_reorder[0]:
                if not is_stockout_period:
                    stock += pending_reorder[1]
                pending_reorder = None

            # 販売による在庫減少
            sold = daily_sales.get((pid, current_date), 0)
            stock = max(0, stock - sold)
            reserved = min(sold, stock)  # 簡略化: 当日の予約 = 販売数

            snap_id += 1
            snapshots.append((
                snap_id, pid, current_date, stock, reserved, reorder_point, lead_time
            ))

            # 発注点チェック
            if stock <= reorder_point and pending_reorder is None and not is_stockout_period:
                reorder_qty = initial_stock  # 初期在庫水準まで補充
                pending_reorder = (day_offset + lead_time, reorder_qty)

    return snapshots


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------
def main():
    rng = random.Random(RANDOM_SEED)

    print("Generating customers...")
    customers = generate_customers(rng)

    print("Generating categories...")
    categories = generate_categories()

    print("Generating products...")
    products = generate_products()

    print("Generating coupons...")
    coupons = generate_coupons(rng)

    print("Generating orders and order items...")
    orders, order_items = generate_orders_and_items(rng, customers, products, coupons)

    print(f"  Orders: {len(orders)}, Order Items: {len(order_items)}")

    print("Generating page views...")
    page_views = generate_page_views(rng, customers, orders, products)
    print(f"  Page Views: {len(page_views)}")

    print("Generating inventory snapshots...")
    inventory_snapshots = generate_inventory_snapshots(rng, products, orders, order_items)
    print(f"  Inventory Snapshots: {len(inventory_snapshots)}")

    # --- Write SQL ---
    print(f"Writing SQL to {OUTPUT_PATH}...")
    buf = StringIO()

    buf.write("-- =============================================================\n")
    buf.write("-- シードデータ投入（自動生成 - scripts/generate_seed.py）\n")
    buf.write("-- 乱数シード: 42（再現性保証）\n")
    buf.write("-- =============================================================\n\n")

    # Categories
    buf.write("-- ----- categories (10件) -----\n")
    write_batch_inserts(buf, "categories",
                        ["category_id", "name", "parent_id"],
                        [(c[0], c[1], c[2]) for c in categories])

    # Customers
    buf.write("-- ----- customers (10,000件) -----\n")
    write_batch_inserts(buf, "customers",
                        ["customer_id", "first_name", "last_name", "email", "phone",
                         "city", "prefecture", "postal_code", "created_at",
                         "gender", "birth_date", "registration_channel"],
                        [(c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8],
                          c[9], c[10], c[11]) for c in customers])

    # Products
    buf.write("-- ----- products (80件) -----\n")
    write_batch_inserts(buf, "products",
                        ["product_id", "product_name", "category_id", "unit_price",
                         "description", "created_at", "cost_price", "weight_gram", "is_active"],
                        [(p[0], p[1], p[2], p[3], p[4], None, p[5], p[6], p[7]) for p in products])

    # Coupons
    buf.write("-- ----- coupons (30件) -----\n")
    write_batch_inserts(buf, "coupons",
                        ["coupon_id", "coupon_code", "coupon_type", "discount_value",
                         "min_order_amount", "valid_from", "valid_to", "campaign_name"],
                        coupons)

    # Orders
    buf.write("-- ----- orders (50,000件) -----\n")
    write_batch_inserts(buf, "orders",
                        ["order_id", "customer_id", "order_date", "shipping_address", "status",
                         "created_at", "updated_at", "order_timestamp", "payment_method",
                         "device_type", "coupon_code"],
                        orders)

    # Order items
    buf.write(f"-- ----- order_items ({len(order_items)}件) -----\n")
    write_batch_inserts(buf, "order_items",
                        ["order_item_id", "order_id", "product_id", "quantity",
                         "unit_price", "discount"],
                        order_items)

    # Page views
    buf.write(f"-- ----- page_views ({len(page_views)}件) -----\n")
    write_batch_inserts(buf, "page_views",
                        ["page_view_id", "customer_id", "session_id", "page_type",
                         "product_id", "category_id", "referrer_type",
                         "view_timestamp", "duration_seconds"],
                        page_views)

    # Inventory snapshots
    buf.write(f"-- ----- inventory_snapshots ({len(inventory_snapshots)}件) -----\n")
    write_batch_inserts(buf, "inventory_snapshots",
                        ["snapshot_id", "product_id", "snapshot_date", "quantity_on_hand",
                         "quantity_reserved", "reorder_point", "lead_time_days"],
                        inventory_snapshots)

    # Reset sequences
    buf.write("-- ----- シーケンスリセット -----\n")
    buf.write(f"SELECT setval('customers_customer_id_seq', {NUM_CUSTOMERS});\n")
    buf.write(f"SELECT setval('categories_category_id_seq', {NUM_CATEGORIES});\n")
    buf.write(f"SELECT setval('products_product_id_seq', {NUM_PRODUCTS});\n")
    buf.write(f"SELECT setval('coupons_coupon_id_seq', {NUM_COUPONS});\n")
    buf.write(f"SELECT setval('orders_order_id_seq', {len(orders)});\n")
    buf.write(f"SELECT setval('order_items_order_item_id_seq', {len(order_items)});\n")
    buf.write(f"SELECT setval('page_views_page_view_id_seq', {len(page_views)});\n")
    buf.write(f"SELECT setval('inventory_snapshots_snapshot_id_seq', {len(inventory_snapshots)});\n")

    # Write to file
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())

    file_size_mb = OUTPUT_PATH.stat().st_size / (1024 * 1024)
    print(f"Done! File size: {file_size_mb:.1f} MB")
    print(f"  Customers: {len(customers)}")
    print(f"  Categories: {len(categories)}")
    print(f"  Products: {len(products)}")
    print(f"  Coupons: {len(coupons)}")
    print(f"  Orders: {len(orders)}")
    print(f"  Order Items: {len(order_items)}")
    print(f"  Page Views: {len(page_views)}")
    print(f"  Inventory Snapshots: {len(inventory_snapshots)}")


if __name__ == "__main__":
    main()
