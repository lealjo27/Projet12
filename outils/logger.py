# outils/logger.py
import os
import logging


def setup_logger(name: str = "checkit_etl", log_file: str = "./logs/etl.log") -> logging.Logger:
    """Configure un logger qui écrit à la fois dans un fichier et dans le terminal."""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Évite de dupliquer les messages si le logger est appelé plusieurs fois
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - [%(name)s] - %(levelname)s - %(message)s")

        # 1. Écriture dans le fichier ./logs/etl.log
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)

        # 2. Affichage dans le terminal
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger