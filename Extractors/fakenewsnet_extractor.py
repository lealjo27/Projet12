# Extractors/fakenewsnet_extractor.py
import os
import sys
from outils.logger import setup_logger
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).resolve().parent.parent))
from outils.image_validator import download_and_validate_image, HEADERS, REQUEST_TIMEOUT
from outils.storage import save_records

logger = setup_logger("fakenewsnet")
DATA_DIR = os.getenv("FAKENEWSNET_DIR", "./data/fakenewsnet")


def _get_image_url(article_url: str) -> str | None:
    if not article_url.startswith("http"):
        article_url = f"https://{article_url}"
    try:
        resp = requests.get(article_url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(resp.text, "html.parser")
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            return og_img["content"]
        container = soup.find("article") or soup
        img = container.find("img")
        return img["src"] if img and img.get("src") else None
    except Exception:
        return None


def _process_article(art_and_label: tuple[dict, str]) -> dict | None:
    art, label = art_and_label
    raw_id = str(art.get("id", "")).strip()
    title = str(art.get("title", "")).strip()
    news_url = str(art.get("news_url", "")).strip()

    if not raw_id or not title or not news_url or news_url == "nan":
        return None

    item_id = f"fnn_{raw_id}"
    img_path = download_and_validate_image(_get_image_url(news_url), item_id)
    if not img_path:
        return None

    return {
        "item_id": item_id,
        "title": title,
        "text_content": title,
        "image_path": img_path,
        "publication_date": None,
        "source_domain": "politifact",
        "label": label,
    }


def fetch_fakenewsnet(limit_per_class: int = 25) -> list[dict]:
    """Extrait rapidement des articles FakeNewsNet en parallèle."""
    logger.info("Démmarrage de l'extraction des articles FakeNewsNet...")
    sources = [
        ("politifact_fake.csv", "fake"),
        ("gossipcop_fake.csv", "fake"),
        ("politifact_real.csv", "real"),
    ]
    records = []

    for filename, label in sources:
        csv_path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(csv_path):
            continue

        articles = pd.read_csv(csv_path).head(limit_per_class * 3).to_dict(orient="records")
        tasks = [(art, label) for art in articles]

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(_process_article, tasks))

        valid = [r for r in results if r is not None][:limit_per_class]
        records.extend(valid)

    logger.info(f"{len(records)} articles FakeNewsNet valides récupérés.")
    return records


if __name__ == "__main__":
    data = fetch_fakenewsnet()
    save_records(data, source_name="fakenewsnet")