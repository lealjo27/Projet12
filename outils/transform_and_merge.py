# outils/transform_and_merge.py
import os
import re
import sys
import glob
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from outils.logger import setup_logger

logger = setup_logger("pipeline_transformation")


# ==============================================================================
# ÉTAPE 1 : LECTURE DES DONNÉES BRUTES
# ==============================================================================
def lire_donnees_brutes(dossier_entree: str = "./data/output") -> pd.DataFrame:
    """Lit et fusionne tous les fichiers Parquet issus des extracteurs."""
    fichiers = glob.glob(os.path.join(dossier_entree, "*", "*.parquet"))
    if not fichiers:
        logger.error(f"Aucun fichier Parquet trouvé dans {dossier_entree}")
        return pd.DataFrame()

    dataframes = []
    for chemin in sorted(fichiers):
        df_source = pd.read_parquet(chemin)
        # Traçabilité de la pipeline d'origine
        df_source["pipeline_source"] = os.path.basename(os.path.dirname(chemin))
        dataframes.append(df_source)
        logger.info(f"Lecture : {chemin} ({len(df_source)} lignes)")

    df_brut = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Total brut chargé : {len(df_brut)} enregistrements.")
    return df_brut


# ==============================================================================
# ÉTAPE 2 : FONCTIONS MODULAIRES DE NETTOYAGE, NORMALISATION & VÉRIFICATION
# ==============================================================================
def nettoie_texte(texte: str) -> str:
    """Nettoie les espaces superflus, sauts de ligne et caractères résiduels."""
    if not isinstance(texte, str):
        return ""
    texte = re.sub(r"<[^>]+>", " ", texte)  # Suppression d'éventuelles balises HTML
    texte = re.sub(r"\s+", " ", texte).strip()  # Normalisation des espaces
    return texte


def normalise_date(date_str: str) -> str | None:
    """Normalise les dates hétérogènes (RFC 2822, ISO) au format ISO 8601 (YYYY-MM-DD)."""
    if not date_str or pd.isna(date_str):
        return None
    date_parsed = pd.to_datetime(date_str, errors="coerce", utc=True)
    if pd.isna(date_parsed):
        return None
    return date_parsed.strftime("%Y-%m-%dT%H:%M:%SZ")


def valide_image(chemin_image: str) -> bool:
    """Vérifie l'association texte-image en contrôlant la présence physique du fichier."""
    return isinstance(chemin_image, str) and os.path.isfile(chemin_image) and os.path.getsize(chemin_image) > 0


def transformer_donnees(df: pd.DataFrame, equilibrer: bool = True, seed: int = 42) -> pd.DataFrame:
    """Applique le nettoyage, le mapping, la génération de features et le rééquilibrage."""
    if df.empty:
        return df

    logger.info("Démarrage des transformations...")

    # 1. Dédoublonnage strict
    nb_avant = len(df)
    df = df.drop_duplicates(subset=["item_id"]).copy()
    logger.info(f"Dédoublonnage : {nb_avant - len(df)} doublon(s) supprimé(s).")

    # 2. Nettoyage et normalisation des champs existants
    df["title"] = df["title"].apply(nettoie_texte)
    df["text_content"] = df["text_content"].apply(nettoie_texte)
    df["publication_date"] = df["publication_date"].apply(normalise_date)
    df["label"] = df["label"].str.lower().str.strip()

    # 3. Vérification de l'intégrité multimodale (Texte non vide + Image présente + Label valide)
    masque_valide = (
        (df["title"].str.len() > 0)
        & df["image_path"].apply(valide_image)
        & df["label"].isin(["real", "fake"])
    )
    df = df[masque_valide].reset_index(drop=True)
    logger.info(f"Après contrôle multimodal (texte + image) : {len(df)} lignes valides.")

    # 4. Mapping et génération de nouvelles colonnes pour le modèle IA (NLP / Vision)
    mapping_label = {"real": 0, "fake": 1}
    df["label_id"] = df["label"].map(mapping_label)  # Cible numérique pour l'entraînement
    df["langue"] = df["pipeline_source"].map({
        "newsdata_fr": "fr",
        "google_factcheck": "fr",
        "franceinfo": "fr",
        "fakenewsnet": "en",
    }).fillna("unknown")
    df["text_length"] = df["text_content"].str.len()  # Feature métadonnée NLP

    # 5. Rééquilibrage reproductible des classes (50 % real / 50 % fake)
    logger.info(f"Distribution avant équilibrage : {df['label'].value_counts().to_dict()}")
    if equilibrer:
        df_real = df[df["label"] == "real"]
        df_fake = df[df["label"] == "fake"]
        nb_min = min(len(df_real), len(df_fake))
        if nb_min > 0:
            df = pd.concat([
                df_real.sample(n=nb_min, random_state=seed),
                df_fake.sample(n=nb_min, random_state=seed),
            ]).sample(frac=1, random_state=seed).reset_index(drop=True)
            logger.info(f"Distribution après équilibrage : {df['label'].value_counts().to_dict()}")

    return df


# ==============================================================================
# ÉTAPE 3 : EXPORTATION REPRODUCTIBLE
# ==============================================================================
def exporter_donnees(df: pd.DataFrame, dossier_final: str = "./data/final", date_exec: str = None) -> dict:
    """Exporte le dataset transformé aux formats Parquet (IA) et JSON (inspection)."""
    if df.empty:
        logger.warning("Aucune donnée à exporter.")
        return {}

    os.makedirs(dossier_final, exist_ok=True)
    tag_date = date_exec or datetime.now().strftime("%Y-%m-%d")

    chemin_parquet = os.path.join(dossier_final, f"dataset_checkit_{tag_date}.parquet")
    chemin_json = os.path.join(dossier_final, f"dataset_checkit_{tag_date}.json")

    df.to_parquet(chemin_parquet, index=False, engine="pyarrow")
    df.to_json(chemin_json, orient="records", force_ascii=False, indent=2)

    logger.info(f"Export terminé : {chemin_parquet} ({len(df)} enregistrements)")
    return {"parquet": chemin_parquet, "json": chemin_json, "count": len(df)}


def executer_pipeline_transformation(date_exec: str = None):
    """Point d'entrée unique orchestrant Lecture -> Transformation -> Export."""
    df_brut = lire_donnees_brutes()
    df_propre = transformer_donnees(df_brut, equilibrer=True, seed=42)
    return exporter_donnees(df_propre, date_exec=date_exec)


if __name__ == "__main__":
    executer_pipeline_transformation()