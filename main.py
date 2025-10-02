import os
import json
from dotenv import load_dotenv
from src.app import ScraperApp


load_dotenv()

WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "8"))
TARGET_URLS = json.loads(os.environ.get("CATEGORIES_URLS", "[]"))
POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST = (
    os.environ.get("POSTGRES_DB"),
    os.environ.get("POSTGRES_USER"),
    os.environ.get("POSTGRES_PASSWORD"),
    os.environ.get("POSTGRES_HOST"),
)


def main():
    """Entrypoint"""
    postgre_dsn = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"
    app = ScraperApp(
        category_urls=TARGET_URLS,
        worker_count=WORKER_COUNT,
        postgre_dsn=postgre_dsn
    )
    app.start()


if __name__ == "__main__":
    main()
