# dags/checkit_etl_dag.py
import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Ajout de la racine Projet_12 au PYTHONPATH pour importer Extractors et outils
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)  # Garantit que les chemins relatifs (./data/...) pointent au bon endroit

from Extractors.newsdata_extractor import fetch_newsdata
from Extractors.factcheck_extractor import fetch_factcheck
from Extractors.fakenewsnet_extractor import fetch_fakenewsnet 
from Extractors.franceinfo_extractor import fetch_franceinfo
from outils.storage import save_records
from outils.transform_and_merge import lire_donnees_brutes, transformer_donnees, exporter_donnees
from outils.db_loader import load_parquet_to_postgres

# ==============================================================================
# FONCTIONS DES TÂCHES (1 tâche = 1 fonction simple et modulaire)
# ==============================================================================
def task_extract_newsdata(**context):
    records = fetch_newsdata()
    save_records(records, source_name="newsdata_fr", execution_date=context["ds"])


def task_extract_factcheck(**context):
    records = fetch_factcheck()
    save_records(records, source_name="google_factcheck", execution_date=context["ds"])


def task_extract_franceinfo(**context):
    records = fetch_franceinfo()
    save_records(records, source_name="franceinfo", execution_date=context["ds"])

def task_extract_fakenewsnet(**context):
    execution_date = context["ds"]
    records = fetch_fakenewsnet()
    return save_records(records, source_name="fakenewsnet", execution_date=execution_date)


def task_transform_data(**context):
    """Reprend les fonctions modulaires de l'Étape 3."""
    df_brut = lire_donnees_brutes()
    df_propre = transformer_donnees(df_brut, equilibrer=True, seed=42)
    res = exporter_donnees(df_propre, date_exec=context["ds"])
    return res.get("parquet")  # Transmis via XCom à la tâche Load


def task_load_to_database(**context):
    """Charge le fichier Parquet transformé dans la base de données (Upsert idempotent)."""
    parquet_path = context["ti"].xcom_pull(task_ids="transform_data")
    if not parquet_path or not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Fichier Parquet introuvable : {parquet_path}")

    df = pd.read_parquet(parquet_path)

    # Connexion locale (remplaçable par PostgresHook(postgres_conn_id="checkit_postgres") en prod)
    db_path = PROJECT_ROOT / "data" / "checkit_warehouse.db"
    with sqlite3.connect(db_path) as conn:
        df.to_sql("publications_multimodales", conn, if_exists="replace", index=False)

def task_load_to_postgres(**context):
    parquet_path = context["ti"].xcom_pull(task_ids="transform_data")
    if not parquet_path:
        raise ValueError("Aucun chemin Parquet reçu depuis transform_and_merge.")
    load_parquet_to_postgres(parquet_path=parquet_path, execution_date=context["ds"])

# ==============================================================================
# DÉFINITION DU DAG AIRFLOW
# ==============================================================================
default_args = {
    "owner": "checkit_data_eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="checkit_multimodal_etl",
    description="Pipeline ETL quotidien d'acquisition de données multimodales (CheckIt.AI)",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["checkit", "etl", "multimodal"],
) as dag:

    t_newsdata = PythonOperator(task_id="extract_newsdata", python_callable=task_extract_newsdata)
    t_factcheck = PythonOperator(task_id="extract_factcheck", python_callable=task_extract_factcheck)
    t_franceinfo = PythonOperator(task_id="extract_franceinfo", python_callable=task_extract_franceinfo)
    t_fakenewsnet = PythonOperator(task_id="extract_fakenewsnet",python_callable=task_extract_fakenewsnet)
    t_transform = PythonOperator(task_id="transform_data", python_callable=task_transform_data)
    # t_load = PythonOperator(task_id="load_to_database", python_callable=task_load_to_database)
    t_load_postgres = PythonOperator(task_id="load_to_postgres", python_callable=task_load_to_postgres)

    # Orchestration : extractions en parallèle -> transformation -> chargement en base
    # [t_newsdata, t_factcheck, t_franceinfo, t_fakenewsnet] >> t_transform >> t_load >> t_load_postgres
    [t_newsdata, t_factcheck, t_franceinfo, t_fakenewsnet] >> t_transform >> t_load_postgres