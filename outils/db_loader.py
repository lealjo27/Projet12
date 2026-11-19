import os
import sys
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent.parent))
from outils.logger import setup_logger

load_dotenv()
logger = setup_logger("postgres_loader")


def get_pg_connection():
    """Ouvre une connexion PostgreSQL avec les paramètres du .env."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "checkit_db"),
        user=os.getenv("PG_USER", "checkit_etl"),
        password=os.getenv("PG_PASSWORD", ""),
    )


def load_parquet_to_postgres(parquet_path: str, execution_date: str) -> int:
    """Charge le fichier Parquet final dans la table PostgreSQL multimodal_articles."""
    if not parquet_path or not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Fichier introuvable : {parquet_path}")

    df = pd.read_parquet(parquet_path)
    if df.empty:
        logger.warning("Dataset vide : rien à insérer.")
        return 0

    df["ingested_at"] = execution_date

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS multimodal_articles (
        item_id VARCHAR(128) PRIMARY KEY,
        title TEXT NOT NULL,
        text_content TEXT,
        image_path TEXT NOT NULL,
        publication_date VARCHAR(64),
        source_domain VARCHAR(128),
        label VARCHAR(16) CHECK (label IN ('real', 'fake')),
        ingested_at DATE NOT NULL
    );
    """

    upsert_sql = """
    INSERT INTO multimodal_articles (
        item_id, title, text_content, image_path,
        publication_date, source_domain, label, ingested_at
    )
    VALUES %s
    ON CONFLICT (item_id) DO UPDATE SET
        title = EXCLUDED.title,
        text_content = EXCLUDED.text_content,
        image_path = EXCLUDED.image_path,
        publication_date = EXCLUDED.publication_date,
        source_domain = EXCLUDED.source_domain,
        label = EXCLUDED.label,
        ingested_at = EXCLUDED.ingested_at;
    """

    records = [
        (
            row["item_id"],
            row["title"],
            row.get("text_content"),
            row["image_path"],
            str(row["publication_date"]) if pd.notna(row.get("publication_date")) else None,
            row.get("source_domain"),
            row["label"],
            row["ingested_at"],
        )
        for _, row in df.iterrows()
    ]

    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            execute_values(cur, upsert_sql, records)
        conn.commit()

    logger.info(f"{len(records)} articles enregistrés dans PostgreSQL (multimodal_articles).")
    return len(records)