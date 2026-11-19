# image_validator.py
import os
import io
import logging
import requests
from PIL import Image
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("CheckItAI_Validator")

# Chemin dynamique depuis le .env
IMAGES_DIR = os.getenv("IMAGES_DIR", "./data/images")
os.makedirs(IMAGES_DIR, exist_ok=True)

# Constantes techniques et règles de qualité d'image
REQUEST_TIMEOUT = 10
MIN_IMAGE_WIDTH = 100
MIN_IMAGE_HEIGHT = 100
HEADERS = {
    "User-Agent": "CheckItAI-DataBot/1.0 (Educational Project)"
}

def download_and_validate_image(image_url: str, item_id: str) -> str | None:
    """
    Télécharge une image, vérifie sa validité avec Pillow et la sauvegarde localement.
    """
    if not image_url or not isinstance(image_url, str):
        return None

    try:
        response = requests.get(image_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").lower()
        if "image" not in content_type:
            logger.warning(f"Rejeté (pas une image : {content_type}) -> {image_url}")
            return None

        image_bytes = io.BytesIO(response.content)
        with Image.open(image_bytes) as img:
            width, height = img.size
            if width < MIN_IMAGE_WIDTH or height < MIN_IMAGE_HEIGHT:
                logger.warning(f"Rejeté (image trop petite {width}x{height}) -> {image_url}")
                return None
            
            file_path = os.path.join(IMAGES_DIR, f"{item_id}.jpg")
            img.convert("RGB").save(file_path, format="JPEG", quality=85)
            
        return file_path

    except Exception as e:
        logger.debug(f"Échec sur l'image {image_url} : {e}")
        return None