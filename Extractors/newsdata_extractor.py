# newsdata_extractor.py
import os
import uuid

import requests
from dotenv import load_dotenv
from outils.image_validator import download_and_validate_image
from outils.storage import save_records

from outils.logger import setup_logger

logger = setup_logger("newsdata")
load_dotenv()


API_KEY = os.getenv("NEWSDATA_API_KEY")


def fetch_newsdata() -> list[dict]:
    """Récupère les articles français avec image depuis NewsData.io."""
    url = f"https://newsdata.io/api/1/latest?apikey={API_KEY}&language=fr&image=1"

    try:
        articles = requests.get(url, timeout=10).json().get("results", [])
    except Exception as e:
        logger.error(f"Erreur API : {e}")
        return []

    records = []
    for art in articles:
        item_id = f"newsdata_{art.get('article_id', uuid.uuid4().hex[:8])}"
        img_path = download_and_validate_image(art.get("image_url"), item_id)

        if art.get("title") and img_path:
            records.append({
                "item_id": item_id,
                "title": art["title"],
                "text_content": art.get("description") or art["title"],
                "image_path": img_path,
                "publication_date": art.get("pubDate"),
                "source_domain": art.get("source_id"),
                "label": "real"
            })
        logger.warning(f"Article ignoré (image invalide ou titre manquant) : {art.get('title', 'N/A')[:50]}...")

    logger.info(f"{len(records)} articles valides récupérés.")
    return records


if __name__ == "__main__":
    data = fetch_newsdata()
    save_records(data, source_name="newsdata_fr")