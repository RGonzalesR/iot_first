import os
import logging
from datetime import datetime


def setup_rotating_log(component: str, default_dir: str) -> logging.Logger:
    """
    Configura logging padronizado para producer/consumer.

    - component: nome do logger ("producer", "consumer", "iot_consumer"...)
    - default_dir: diretório de logs (ex.: /app/volumes/kafka/logs)

    Usa a env <COMPONENT>_LOG_DIR se existir, senão usa default_dir.
    Cria o diretório se não existir.
    Cria arquivo: log_<component>_<YYYYMMDD_HHMMSS>.log
    Retorna o logger configurado.
    """

    # 1) Diretório de log
    env_var = f"{component.upper()}_LOG_DIR"
    log_dir = os.getenv(env_var, default_dir)
    os.makedirs(log_dir, exist_ok=True)

    # 2) Nome do arquivo com timestamp
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    logfile = os.path.join(log_dir, f"log_{component}_{ts}.log")

    # 3) Recupera sempre o mesmo logger pelo nome
    logger = logging.getLogger(component)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # evita duplicar logs no root

    # 4) Limpa handlers antigos (se já tiver sido configurado)
    logger.handlers.clear()

    # 5) Cria handlers e formatter
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(logfile, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"[logging] Arquivo de log inicializado: {logfile}")

    return logger
