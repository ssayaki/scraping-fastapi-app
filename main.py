from fastapi import FastAPI, Response
from pydantic import BaseModel
from typing import List
import requests
from duckduckgo_search import DDGS
from bs4 import BeautifulSoup
import re
import logging
import os
import json
from collections import Counter
import time
import random


logging.basicConfig(level=logging.INFO)
app = FastAPI()

DIFY_API_URL = "https://api.dify.ai/v1/workflows/run"
DIFY_API_KEY = os.getenv("DIFY_API_KEY")

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
    "卸売業、小売業": ["販売", "小売", "卸", "通販", "店舗"],
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

def extract_industry_and_prefecture(text):
    keyword_hits = []
    for name, keywords in industry_keywords.items():
        for kw in keywords:
            if kw in text:
                keyword_hits.append(name)

    industry = "分類不能の産業"
    if keyword_hits:
        industry = Counter(keyword_hits).most_common(1)[0][0]

    prefecture = ""
    match = re.search(r"(本社所在地|本社|所在地|住所|事業所|〒)?[^。・\n\r]{0,20}?" + pref_pattern, text)
    if match:
        prefecture = match.group(2 if match.lastindex == 2 else 1)
    else:
        match = re.search(pref_pattern, text)
        if match:
            prefecture = match.group(1)

    return industry, prefecture

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

            try:
                results = list(ddgs.text(query, region="jp-jp", max_results=1))
                if results:
                    url = results[0].get("href", "")
                    snippet_text = results[0].get("body", "")
                    info = snippet_text
            except Exception as e:
                logging.warning(f"[STEP 1] DuckDuckGo検索失敗: {company_name}: {e}")

            time.sleep(random.uniform(2, 3))  # 2〜3秒ランダム待機

            industry, prefecture = extract_industry_and_prefecture(info)

            if industry == "分類不能の産業" or prefecture == "":
                try:
                    res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
                    res.encoding = res.apparent_encoding
                    if res.status_code == 200:
                        soup = BeautifulSoup(res.text, "html.parser")
                        text = soup.get_text()
                        industry, prefecture = extract_industry_and_prefecture(text)
                except Exception as e:
                    logging.warning(f"[STEP 2] URL取得エラー: {company_name}:　{e} ({url})")

            target_text = text if text else info
            dify_context = ""
            if industry == "分類不能の産業":
                match = re.search(r"(業務内容|事業内容|サービス|施工.*?)[^\n]{0,100}", target_text)
                if match:
                    start = max(0, match.start() - 100)
                    end = min(len(target_text), match.end() + 200)
                    dify_context = target_text[start:end].strip()
                else:
                    dify_context = target_text[:300].strip()

            enriched_items.append({
                "company_name": company_name,
                "email": email,
                "url": url,
                "industry": industry,
                "prefecture": prefecture,
                "_text_excerpt": dify_context  # 内部用
            })

    dify_targets = [item for item in enriched_items if item["industry"] == "分類不能の産業"]

    if dify_targets:
        dify_payload = {
            "inputs": {
                "industry_texts": payload.industry_texts,
                "info_list": json.dumps([
                    {"company_name": item["company_name"], "info": item["_text_excerpt"]}
                    for item in dify_targets
                ], ensure_ascii=False)
            },
            "response_mode": "blocking",
            "user": "company-fetcher"
        }
        logging.info(f"[DIFY PAYLOAD]: {json.dumps(dify_payload, ensure_ascii=False, indent=2)}")

        headers = {
            "Authorization": f"Bearer {DIFY_API_KEY}",
            "Content-Type": "application/json"
        }

        # try:
        #     dify_response = requests.post(DIFY_API_URL, headers=headers, json=dify_payload)
        #     dify_response.raise_for_status()
        #     dify_result = dify_response.json()
        #     logging.info(f"[DIFY RAW RESPONSE]: {json.dumps(dify_result, ensure_ascii=False)}")
        #     results_str = dify_result.get("data", {}).get("outputs", {}).get("results", "[]")
        #     predictions = json.loads(results_str)
        #     logging.info(f"[DIFY PARSED PREDICTIONS]: {predictions}")
        # except Exception as e:
        #     logging.error(f"[DIFY ERROR] 呼び出し or 結果のパースに失敗: {e}")
        #     predictions = []

        # for item in enriched_items:
        #     if item["industry"] == "分類不能の産業":
        #         matched = next((p["industry"] for p in predictions if p["company_name"] == item["company_name"]), "")
        #         item["industry"] = matched
        #         item.pop("_text_excerpt", None)

    return enriched_items
