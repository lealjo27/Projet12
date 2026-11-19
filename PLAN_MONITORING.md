# Plan de Monitoring et d'Observabilité — Pipeline ETL CheckIt.AI

**Projet :** CheckIt.AI — Détection multimodale de désinformation (Texte + Image)  
**Pipeline surveillé :** `checkit_multimodal_etl` (Apache Airflow)  
**Version :** 1.1

---

## 1. Objectifs et périmètre du monitoring

Ce plan définit la stratégie de surveillance en production du pipeline d'ingestion multimodale **CheckIt.AI**. Il vise à garantir trois engagements opérationnels :

1. **Disponibilité et fiabilité de l'orchestration :** S'assurer que le DAG quotidien s'exécute de bout en bout sans interruption silencieuse.
2. **Intégrité et qualité des données IA :** Détecter toute dérive de volume, perte d'équilibre entre classes (`real` / `fake`) ou dégradation de la qualité des images avant l'entraînement des modèles.
3. **Maîtrise des coûts et des ressources :** Prévenir le dépassement des quotas d'APIs gratuites et la saturation de l'espace disque ou de la base PostgreSQL (`checkit_db`).

---

## 2. Architecture de supervision (3 niveaux d'observabilité)

Le pipeline suit l'architecture parallélisée orchestrée par Apache Airflow (`schedule="@daily"`, `retries=2`, `retry_delay=2 min`) :

```text
[extract_newsdata]   ──┐
[extract_factcheck]  ──┼──▶ [transform_and_merge] ──▶ [load_to_postgres]
[extract_fakenewsnet]──┤
[extract_franceinfo] ──┘
```

La surveillance s'articule autour de **trois couches complémentaires** :

| Couche | Outils & Emplacements | Rôle dans la supervision |
| :--- | :--- | :--- |
| **1. Orchestrateur** | **Apache Airflow UI** (`http://localhost:8080`) & `./airflow_home/logs/dag_id=checkit_multimodal_etl/` | Suivi des états d'exécution (`success`, `up_for_retry`, `failed`), respect des SLA, durée par tâche et traces d'exceptions Python. |
| **2. Applicatif & Métier** | **Logger centralisé** (`outils/logger.py` $\rightarrow$ `./logs/etl.log`) | Journalisation horodatée par module (`[newsdata]`, `[factcheck]`, `[fakenewsnet]`, `[franceinfo]`, `[transform]`, `[db_loader]`) avec niveaux `INFO`, `WARNING`, `ERROR`. |
| **3. Données & Décisionnel** | **Tableau de bord Streamlit** (`dashboard_kpi.py`) & **PostgreSQL** (`checkit_db.multimodal_articles`) | Contrôle visuel des KPI qualité (complétude $100\%$, parité $50/50$, résolution images $\ge 224\times 224\text{ px}$), audit des doublons et suivi de l'empreinte disque. |

---

## 3. Indicateurs Clés de Performance (KPI) surveillés

| Axe | Indicateur (KPI) | Formule / Source de mesure | Cible nominale |
| :--- | :--- | :--- | :--- |
| **Précision & Qualité** | **Taux de complétude du schéma** | $\%$ de lignes sans valeur nulle sur `item_id`, `title`, `text_content`, `image_path`, `label` | $100\%$ |
| **Précision & Qualité** | **Parité des classes (`real` / `fake`)** | Ratio $\frac{\min(N_{\text{real}}, N_{\text{fake}})}{\max(N_{\text{real}}, N_{\text{fake}})}$ après `transform_and_merge` | $100\%$ ($50\% / 50\%$) |
| **Précision & Qualité** | **Compatibilité Vision IA** | $\%$ d'images locales validées par Pillow avec dimensions $\ge 224 \times 224\text{ px}$ | $\ge 90\%$ |
| **Précision & Qualité** | **Taux de rétention équilibrage** | $\frac{\text{Articles retenus en final}}{\text{Articles uniques valides extraits}} \times 100$ | $\ge 50\%$ |
| **Rapidité** | **Latence totale du DAG (E2E)** | Durée entre le démarrage des 4 extractions parallèles et la fin de `load_to_postgres` | $< 3\text{ min}$ |
| **Rapidité** | **Durée maximale par tâche** | Temps isolé d'une tâche (`extract_*`, `transform_and_merge`, `load_to_postgres`) | $< 60\text{ s}$ |
| **Coût & Ressources** | **Consommation quotas API** | Crédits NewsData.io consommés / 200 crédits quotidiens gratuits | $< 20\%\text{ / jour}$ |
| **Coût & Ressources** | **Empreinte stockage** | Taille cumulée de `./data/images/` + `./data/final/*.parquet` + base `checkit_db` | $< 50\text{ Mo / mois}$ |

---

## 4. Fréquences de vérification et rituels opérationnels

Le calendrier de contrôle est calé sur la planification quotidienne (`schedule="@daily"`) du DAG Airflow :

| Fréquence | Périmètre vérifié | Méthode / Outil | Action attendue |
| :--- | :--- | :--- | :--- |
| **Temps réel (à chaque exécution)** | Statut des 6 tâches du DAG et exceptions critiques | Callbacks Airflow (`on_retry_callback`, `on_failure_callback`) + logs | Notification immédiate en cas d'échec définitif après `retries=2`. |
| **Quotidienne** *(après le run `@daily`, ~02h05 UTC)* | Volume inséré, équilibre `50/50` et absence d'erreurs `ERROR` | Assertions dans `transform_and_merge.py` + onglet **Airflow, Logs & Erreurs** sur Streamlit | Vérifier que `dataset_checkit_YYYY-MM-DD.parquet` et PostgreSQL sont synchronisés. |
| **Hebdomadaire** *(chaque lundi)* | Taux de rejet d'images (`WARNING`), dérive de longueur de texte (NLP) et doublons | Onglets **Sources**, **Qualité IA (NLP & Vision)** et **Audit des Doublons** sur Streamlit | Identifier une source qui se tarit ou un changement de structure HTML/RSS. |
| **Mensuelle** | Espace disque (`/home` et `/var/lib/pgsql/data/`), rotation des logs et maintenance SQL | Script système + commande PostgreSQL `VACUUM ANALYZE multimodal_articles;` | Archiver ou purger les logs de plus de 30 jours et contrôler la croissance disque. |

---

## 5. Matrice des seuils d'alerte et niveaux de criticité

| Métrique / Événement | Niveau 🟢 OK | Niveau 🟠 Warning (Avertissement) | Niveau 🔴 Critical (Intervention requise) |
| :--- | :--- | :--- | :--- |
| **Exécution des tâches Airflow** | Toutes les tâches en `success` au 1ᵉʳ essai | 1 tâche en `up_for_retry` (reprise auto réussie) | Tâche en état `failed` après épuisement des `retries=2` |
| **Erreurs dans `./logs/etl.log`** | `0` ligne `ERROR` | Hausse des `WARNING` ($> 30\%$ d'images rejetées) | $\ge 1$ ligne `ERROR` bloquante ou `0` article extrait |
| **Parité des classes (`real` / `fake`)** | Ratio $= 100\%$ ($50\% / 50\%$) | Ratio entre $95\%$ et $99\%$ | Ratio $< 90\%$ ou une classe tombée à `0` article |
| **Taux de rétention après équilibrage** | $\ge 50\%$ | Entre $30\%$ et $49\%$ (déséquilibre à la source) | $< 30\%$ des articles bruts conservés |
| **Latence totale du DAG (SLA)** | $< 3\text{ min}$ | Entre $3\text{ min}$ et $8\text{ min}$ | $> 10\text{ min}$ (dépassement du SLA quotidien) |
| **Quotas API (NewsData / FactCheck)** | $< 20\%$ du quota/jour | Entre $50\%$ et $80\%$ du quota/jour | Code `HTTP 429 Too Many Requests` reçu |
| **Espace disque disponible** | $> 20\%$ libre | Entre $10\%$ et $20\%$ libre | $< 10\%$ libre (risque d'échec écriture images/BDD) |

---

## 6. Stratégie de gestion des erreurs et procédures de remédiation

### 6.1. Mécanismes automatiques de résilience (Sans intervention humaine)

1. **Reprises automatiques ciblées (`retries=2`) :** Configuré dans `default_args` du DAG avec `retry_delay=timedelta(minutes=2)`. En cas de micro-coupure réseau, Airflow relance uniquement la tâche défaillante sans ré-exécuter les tâches déjà réussies.
2. **Isolation des sources parallèles :** Les 4 extracteurs (`newsdata`, `factcheck`, `fakenewsnet`, `franceinfo`) étant indépendants, l'indisponibilité temporaire d'une source n'interrompt pas la collecte des trois autres.
3. **Filtrage unitaire non bloquant :** Dans `outils/image_validator.py`, une URL d'image morte (`404`/`403`), un `timeout=10s` ou une image $< 100\text{ px}$ génère un `WARNING` dans `./logs/etl.log` et ignore uniquement l'article concerné sans faire planter le lot.
4. **Idempotence des écritures :**
   * Fichiers finaux horodatés (`./data/final/dataset_checkit_YYYY-MM-DD.parquet`).
   * Insertion PostgreSQL via `ON CONFLICT (item_id) DO UPDATE`, garantissant l'absence de doublons lors d'une relance manuelle ou d'un rattrapage (*backfill*).

### 6.2. Runbook d'intervention en cas d'alerte critique (🔴 Critical)

| Scénario d'incident | Symptôme observé | Diagnostic et action corrective |
| :--- | :--- | :--- |
| **1. Quota API épuisé** | `HTTP 429` dans `./logs/etl.log` sur `newsdata` ou `factcheck` | Réduire temporairement le paramètre `limit` / `max_pages` dans l'extracteur concerné et attendre la réinitialisation quotidienne des crédits à `00:00 UTC`. |
| **2. Changement de structure HTML/RSS** | `AttributeError`, `ParseError` ou `0` article récupéré sur `franceinfo` | Inspecter le flux RSS / sélecteurs BeautifulSoup dans `Extractors/franceinfo_extractor.py`, corriger le parsing, puis cliquer sur **Clear Task** dans Airflow UI. |
| **3. Blocage téléchargement images** | Taux de rejet $> 60\%$ (`HTTP 403`) dans `./logs/etl.log` | Vérifier la validité de `USER_AGENT` dans `.env` et contrôler l'expiration des URLs sources (`fakeddit` / `fakenewsnet`). |
| **4. Base PostgreSQL indisponible** | `psycopg2.OperationalError` sur la tâche `load_to_postgres` | Vérifier le service (`sudo systemctl status postgresql`), le redémarrer si nécessaire (`sudo systemctl restart postgresql`), puis relancer uniquement `load_to_postgres` (opération 100 % idempotente). |
| **5. Saturation disque** | Alerte disque $< 10\%$ libre | Purger les anciens fichiers temporaires `./data/output/*.parquet` de plus de 30 jours et exécuter `VACUUM FULL` sur `checkit_db`. |

---

## 7. Implémentation des callbacks d'alerte dans Airflow

Pour automatiser la remontée des alertes critiques dès l'échec d'une tâche, le DAG `airflow_home/dags/checkit_etl_dag.py` intègre une fonction de rappel (`on_failure_callback`) dans `default_args` :

```python
from outils.logger import setup_logger

alert_logger = setup_logger("airflow_alerts")

def on_task_failure_alert(context):
    """Callback déclenché automatiquement par Airflow lors d'un échec définitif."""
    ti = context.get("task_instance")
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get("ds")
    exception = context.get("exception")

    alert_logger.error(
        f"[ALERTE CRITIQUE AIRFLOW] DAG={dag_id} | Tâche={task_id} | "
        f"Date={execution_date} | Erreur={exception}"
    )
```

Cette trace est immédiatement remontée dans le tableau de bord Streamlit (`dashboard_kpi.py`, onglet **⚡ Airflow, Logs & Erreurs**) et peut être couplée à un envoi d'email (`EmailOperator` / SMTP) ou un webhook Slack/Teams.