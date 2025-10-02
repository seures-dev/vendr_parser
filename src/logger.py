import logging


def get_logger(module_name: str):
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    fh = logging.FileHandler("app.log", mode="a")
    fh.setFormatter(formatter)

    if not logger.handlers: 
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger
