# factcheck_extractor.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import os
import hashlib
import logging
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from outils.image_validator import download_and_validate_image, HEADERS, REQUEST_TIMEOUT
from outils.storage import save_records
from outils.logger import setup_logger


logger = setup_logger("factcheck")

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

API_KEY = os.getenv("GOOGLE_FACTCHECK_API_KEY")


def _get_image_url(article_url: str) -> str | None:
    """Récupère l'image principale (og:image) ou la première image de l'article."""
    try:
        resp = requests.get(article_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(resp.text, "html.parser")

        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            return og_img["content"]

        container = soup.find("article") or soup
        img = container.find("img")
        if img and img.get("src"):
            return img["src"]

        return None
    except Exception:
        return None


def _get_label(textual_rating: str) -> str | None:
    """Retourne 'real', 'fake', ou None si le verdict est ambigu."""
    rating = textual_rating.lower().strip()
    if any(w in rating for w in ["vrai", "exact", "correct", "authentique"]):
        return "real"
    if any(w in rating for w in ["faux", "fausse", "trompeur", "infox", "intox", "falsifié", "manipulé"]):
        return "fake"
    return None


def fetch_factcheck(query: str = "vaccin OR climat OR élection OR france") -> list[dict]:
    """Récupère les articles de fact-checking français classés 'real' ou 'fake'."""
    if not API_KEY:
        logger.error("Clé API manquante dans le fichier .env !")
        return []

    url = "https://factchecktools.googleapis.com/v1alpha1/claims:search"
    params = {"key": API_KEY, "languageCode": "fr", "query": query, "pageSize": 10}

    try:
        articles = requests.get(url, params=params, timeout=REQUEST_TIMEOUT).json().get("claims", [])
    except Exception as e:
        logger.error(f"Erreur API : {e}")
        return []

    records = []
    for art in articles:
        review = art.get("claimReview", [{}])[0]
        article_url = review.get("url", "")
        claim_text = art.get("text", "").strip()
        label = _get_label(review.get("textualRating", ""))

        if not label or not article_url or not claim_text:
            continue

        url_hash = hashlib.md5(article_url.encode("utf-8")).hexdigest()[:8]
        item_id = f"factcheck_{url_hash}"
        img_path = download_and_validate_image(_get_image_url(article_url), item_id)

        if img_path:
            records.append({
                "item_id": item_id,
                "title": review.get("title") or claim_text[:80],
                "text_content": claim_text,
                "image_path": img_path,
                "publication_date": review.get("reviewDate"),
                "source_domain": review.get("publisher", {}).get("name", "unknown"),
                "label": label
            })
        else:
            logger.warning(f"Article ignoré (image invalide) : {claim_text[:50]}...")

    logger.info(f"{len(records)} articles valides récupérés.")
    return records


if __name__ == "__main__":
    data = fetch_factcheck()
    save_records(data, source_name="google_factcheck")