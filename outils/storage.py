# storage.py
import os
import json
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("CheckItAI_Storage")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data/output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_records(records: list[dict], source_name: str, execution_date: str = None) -> dict:
    """
    Sauvegarde une liste d'enregistrements multimodaux validés aux formats JSON et Parquet.
    Garantit la déduplication sur 'item_id' et l'idempotence par date d'exécution.
    
    Retourne un dictionnaire contenant les chemins des fichiers créés (utile pour Airflow XCom).
    """
    if not records:
        logger.warning(f"[{source_name}] Aucun enregistrement à stocker.")
        return {}

    # 1. Conversion en DataFrame et déduplication sur l'identifiant unique
    df = pd.DataFrame(records)
    initial_count = len(df)
    df.drop_duplicates(subset=["item_id"], keep="last", inplace=True)
    if len(df) < initial_count:
        logger.info(f"[{source_name}] {initial_count - len(df)} doublon(s) supprimé(s).")

    # 2. Gestion de la date (permet l'idempotence si relancé le même jour sous Airflow)
    date_tag = execution_date or datetime.now().strftime("%Y-%m-%d")
    source_dir = os.path.join(OUTPUT_DIR, source_name)
    os.makedirs(source_dir, exist_ok=True)

    # 3. Stockage au format JSON (lisible humainement / semi-structuré)
    json_path = os.path.join(source_dir, f"{source_name}_{date_tag}.json")
    df.to_json(json_path, orient="records", force_ascii=False, indent=4)
    logger.info(f"[{source_name}] Stockage JSON réussi : {json_path} ({len(df)} lignes)")

    # 4. Stockage au format Apache Parquet (optimisé pour le Machine Learning)
    parquet_path = os.path.join(source_dir, f"{source_name}_{date_tag}.parquet")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"[{source_name}] Stockage Parquet réussi : {parquet_path}")

    return {
        "json_path": json_path,
        "parquet_path": parquet_path,
        "record_count": len(df)
    }