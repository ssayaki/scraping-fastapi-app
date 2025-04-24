from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import requests
from duckduckgo_search import DDGS
from bs4 import BeautifulSoup
import re
import logging
import os
from collections import Counter, defaultdict
import time
import random


logging.basicConfig(level=logging.INFO)
app = FastAPI()

# DIFY_API_URL = "https://api.dify.ai/v1/workflows/run"
# DIFY_API_KEY = os.getenv("DIFY_API_KEY")

# 業種分類用の簡易キーワードルール（19業種のみ）
industry_keywords = {
    "農業・林業": ["農業", "林業", "栽培", "畜産", "伐採"],
    "漁業": ["漁業", "水産", "養殖", "漁港"],
    "鉱業、採石業、砂利採取業": ["採石", "鉱山", "採掘", "鉱業所"],
    "建設業": ["施工", "工事", "解体", "リフォーム", "内装", "外壁", "設計"],
    "製造業": ["製造", "加工", "工場", "生産", "部品"],
    "電気・ガス・熱供給・水道業": ["電力", "ガス", "水道", "インフラ"],
    "情報通信業": ["システム開発", "IT", "クラウド", "アプリ", "ソフトウェア"],
    "運輸業、郵便業": ["運送", "物流", "配送", "トラック", "郵便"],
    "卸売業、小売業": ["販売", "小売", "卸", "通販"],
    "金融業、保険業": ["保険", "金融", "銀行", "証券", "ローン"],
    "不動産業、物品賃貸業": ["不動産", "賃貸", "仲介", "物件"],
    "学術研究、専門・技術サービス業": ["研究", "開発", "技術支援", "コンサル"],
    "宿泊業、飲食サービス業": ["ホテル", "旅館", "飲食", "レストラン"],
    "生活関連サービス業、娯楽業": ["美容院", "マッサージ", "娯楽", "エステ", "ペット"],
    "教育、学習支援業": ["教育", "学校", "学習", "塾", "研修"],
    "医療、福祉": ["クリニック", "病院", "介護", "看護", "福祉施設"],
    "複合サービス事業": ["郵便局", "農協", "生協", "漁協"],
    "サービス業（他に分類されないもの）": ["清掃", "警備", "レンタル", "代行", "メンテナンス"],
    "公務（他に分類されるものを除く)": ["市役所", "官公庁", "自治体", "行政"]
}

pref_pattern = r"(東京都|北海道|(?:京都|大阪)府|.{2,3}県)"

def extract_industry(text):
    keyword_hits = []
    keyword_map = defaultdict(set)

    for name, keywords in industry_keywords.items():
        for kw in keywords:
            if kw in text:
                # 金融ワードだけ、文脈チェックで除外する
                if name == "金融業、保険業":
                    # 「取引先」「振込先」などの直後に金融ワードがある場合はスキップ
                    pattern = r"(取引先|取引銀行|振込先|協力|融資先|口座).*?" + re.escape(kw)
                    if re.search(pattern, text):
                        continue # 誤判定の可能性があるため無視する
                keyword_hits.append(name)
                keyword_map[name].add(kw)

    industry = "分類不能の産業"
    certainty = ""
    if keyword_hits:
        count = Counter(keyword_hits)
        max_count = max(count.values())
        candidates = [k for k, v in count.items() if v == max_count]
        industry = ", ".join(candidates)
        if len(candidates) == 1 and max_count >= 2:
            certainty = "確定"

    keyword_map = {k: list(v) for k, v in keyword_map.items()}
    return industry, keyword_map, certainty

def extract_prefecture(text):
    prefecture = ""
    match = re.search(r"(本社所在地|本社|所在地|住所|事業所|〒)?[^。・\n\r]{0,20}?" + pref_pattern, text)
    # 最初に「住所」や「〒」の直後に都道府県が出てくるものを優先
    if match:
        prefecture = re.search(pref_pattern, match.group()).group(1)
    else:
        # fallback
        match = re.search(pref_pattern, text)
        if match:
            prefecture = match.group(1)
    return prefecture


class CompanyItem(BaseModel):
    company_name: str
    phone_number: str
    email: str

class RequestPayload(BaseModel):
    items: List[CompanyItem]
    industry_texts: str

@app.get("/")
def root():
    return {"message": "POSTリクエストでデータを送信してください。"}

@app.post("/")
async def handle_batch_request(payload: RequestPayload):
    enriched_items = []

    with DDGS() as ddgs:
        for item in payload.items:
            company_name = item.company_name
            phone_number = item.phone_number
            email = item.email
            query = f"{company_name} {phone_number} {email}".strip()

            url, snippet_text, text, info = "", "", "", ""
            log_messages = []

            try:
                results = list(ddgs.text(query, region="jp-jp", max_results=1))
                if results:
                    url = results[0].get("href", "")
                    snippet_text = results[0].get("body", "")
                    info = snippet_text
                else:
                    log_messages.append("DuckDuckGo検索結果なし")
            except Exception as e:
                log_messages.append(f"DuckDuckGo検索失敗: {e}")

            time.sleep(random.uniform(2, 3))

            # 業種と都道府県が抽出できなかった場合、snippetから抽出
            industry, keyword_map, certainty = extract_industry(info)
            prefecture = extract_prefecture(info)
            # 業種が未確定 or 都道府県が抽出できなかった場合、ページ内容から抽出
            if certainty != "確定" or prefecture == "":
                try:
                    res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=(5, 15))
                    res.encoding = res.apparent_encoding
                    if res.status_code == 200:
                        soup = BeautifulSoup(res.text, "html.parser")
                        text = soup.get_text()
                        if certainty != "確定":
                            industry, keyword_map, certainty = extract_industry(text)
                        if prefecture == "":
                            prefecture = extract_prefecture(text)
                    else:
                        log_messages.append(f"URL取得失敗（ステータスコード: {res.status_code}）")
                except Exception as e:
                    log_messages.append(f"URL取得エラー: {e} ({url})")

            if industry == "分類不能の産業":
                log_messages.append("業種分類不能")
            if prefecture == "":
                log_messages.append("都道府県抽出失敗")

            # 業種判定に役立つ文言の前後を抽出
            target_text = text if text else info
            dify_context = ""
            if certainty != "確定":
                match = re.search(r"(業務内容|事業内容|サービス|施工.*?|事業|提供|届ける|ために)[^\n]{0,100}", target_text)
                if match:
                    start = max(0, match.start() - 100)
                    end = min(len(target_text), match.end() + 300)
                    dify_context = target_text[start:end].strip()
                else:
                    dify_context = target_text[:500].strip()

            enriched_items.append({
                "company_name": company_name,
                "email": email,
                "url": url,
                "industry": industry,
                "prefecture": prefecture,
                "keywords": keyword_map,
                "certainty": certainty,
                "_text_excerpt": dify_context,
                "log": log_messages
            })

    return enriched_items
