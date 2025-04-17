from fastapi import FastAPI, Response
from pydantic import BaseModel
from typing import List
import requests
from duckduckgo_search import DDGS
from bs4 import BeautifulSoup
import re
import os
import logging
import json

logging.basicConfig(level=logging.INFO)
app = FastAPI()

DIFY_API_URL = "https://api.dify.ai/v1/workflows/run"
DIFY_API_KEY = os.getenv("DIFY_API_KEY")

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

@app.get("/favicon.ico")
def favicon():
    return Response(content="", media_type="image/x-icon")

@app.post("/")
async def handle_batch_request(payload: RequestPayload):
    enriched_items = []

    with DDGS() as ddgs:
        for item in payload.items:
            company_name = item.company_name
            phone_number = item.phone_number
            email = item.email
            query = f"{company_name} {phone_number} {email}".strip()

            url, snippet_text, info = "", "", ""
            prefecture = ""

            try:
                results = list(ddgs.text(query, region="jp-jp", max_results=1))
                if results:
                    url = results[0].get("href", "")
                    snippet_text = results[0].get("body", "")
                    info = snippet_text
            except Exception as e:
                logging.warning(f"[STEP 1] DuckDuckGo検索失敗: {e}")

            logging.info(f"[STEP 1] URL: {url}, Snippet: {snippet_text}")

            try:
                res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=7)

                if res.status_code == 200:
                    html = res.text
                    soup = BeautifulSoup(html, "html.parser")
                    text = soup.get_text()

                    match = re.search(r"(東京都|北海道|(?:京都|大阪)府|.{2,3}県)", text)
                    if match:
                        prefecture = match.group(1)
                else:
                    logging.warning(f"[STEP 2] URLアクセス失敗（ステータス: {res.status_code}）: {url}")
                    # snippetから都道府県抽出を試みる（フォールバック）
                    match = re.search(r"(東京都|北海道|(?:京都|大阪)府|.{2,3}県)", snippet_text)
                    if match:
                        prefecture = match.group(1)

            except Exception as e:
                logging.warning(f"[STEP 2] URL取得エラー: {e} ({url})")
                # snippetから都道府県抽出を試みる（フォールバック）
                match = re.search(r"(東京都|北海道|(?:京都|大阪)府|.{2,3}県)", snippet_text)
                if match:
                    prefecture = match.group(1)

            # URLが閉鎖されていても append は必ず実施
            enriched_items.append({
                "company_name": company_name,
                "email": email,
                "url": url,
                "prefecture": prefecture,
                "info": info
            })


    dify_payload = {
        "inputs": {
            "industry_texts": payload.industry_texts,
            "info_list": json.dumps([
                {"company_name": item["company_name"], "info": item["info"]}
                for item in enriched_items
            ], ensure_ascii=False)
        },
        "response_mode": "blocking",
        "user": "company-fetcher"
    }

    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        dify_response = requests.post(DIFY_API_URL, headers=headers, json=dify_payload)
        dify_response.raise_for_status()
        dify_result = dify_response.json()
        logging.info(f"[DIFY RAW RESPONSE]: {json.dumps(dify_result, ensure_ascii=False)}")
        results_str = dify_result.get("data", {}).get("outputs", {}).get("results", "[]")
        predictions = json.loads(results_str)
        logging.info(f"[DIFY PARSED PREDICTIONS]: {predictions}")
    except Exception as e:
        logging.error(f"[DIFY ERROR] 呼び出し or 結果のパースに失敗: {e}")
        predictions = []

    results = []
    for item in enriched_items:
        matched = next((p["industry"] for p in predictions if p["company_name"] == item["company_name"]), "")
        results.append({
            "company_name": item["company_name"],
            "email": item["email"],
            "url": item["url"],
            "prefecture": item["prefecture"],
            "industry": matched
        })

    return results