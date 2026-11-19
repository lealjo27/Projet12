# franceinfo_extractor.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import hashlib
import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup
from outils.image_validator import download_and_validate_image, HEADERS, REQUEST_TIMEOUT
from outils.storage import save_records
from outils.logger import setup_logger

logger = setup_logger("franceinfo")



RSS_URL = "https://www.francetvinfo.fr/titres.rss"


def _get_image_url(item: ET.Element, article_url: str) -> str | None:
    """Récupère l'image depuis la balise <enclosure> du RSS ou via og:image."""
    enclosure = item.find("enclosure")
    if enclosure is not None and enclosure.get("url"):
        return enclosure.get("url")

    try:
        resp = requests.get(article_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(resp.text, "html.parser")
        og_img = soup.find("meta", property="og:image")
        return og_img["content"] if og_img and og_img.get("content") else None
    except Exception:
        return None


def fetch_franceinfo(rss_url: str = RSS_URL, limit: int = 20) -> list[dict]:
    """Extrait les dépêches d'actualité multimodales depuis le flux RSS de France Info."""
    logger.info("Démmarrage de l'extraction des articles France Info...")
    try:
        resp = requests.get(rss_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        articles = root.findall(".//item")
    except Exception as e:
        logger.error(f"Erreur lecture RSS France Info : {e}")
        return []

    records = []
    for art in articles:
        if len(records) >= limit:
            break

        title = (art.findtext("title") or "").strip()
        description = (art.findtext("description") or "").strip()
        link = (art.findtext("link") or "").strip()
        pub_date = (art.findtext("pubDate") or "").strip()

        # On écarte les articles sans titre/lien ainsi que la rubrique "Vrai ou Faux"
        if not title or not link or "vrai-ou-faux" in link.lower():
            continue

        clean_desc = BeautifulSoup(description, "html.parser").get_text(" ", strip=True)
        text_content = f"{title}. {clean_desc}" if clean_desc else title

        url_hash = hashlib.md5(link.encode("utf-8")).hexdigest()[:8]
        item_id = f"franceinfo_{url_hash}"
        img_path = download_and_validate_image(_get_image_url(art, link), item_id)

        if img_path:
            records.append({
                "item_id": item_id,
                "title": title,
                "text_content": text_content,
                "image_path": img_path,
                "publication_date": pub_date,
                "source_domain": "francetvinfo.fr",
                "label": "real"
            })
        else:
            logger.warning(f"Article ignoré (pas d'image valide) : {link}")

    logger.info(f"{len(records)} articles France Info valides récupérés.")
    return records


if __name__ == "__main__":
    data = fetch_franceinfo()
    save_records(data, source_name="franceinfo")