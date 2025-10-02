import os
import json
from dotenv import load_dotenv
from src.app import ScraperApp


load_dotenv()

WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "8"))
TARGET_URLS = json.loads(os.environ.get("CATEGORIES_URLS", "[]"))


def main():
    """Entrypoint"""
    app = ScraperApp(
        category_urls=TARGET_URLS,
        worker_count=WORKER_COUNT,
    )
    app.start()


if __name__ == "__main__":
    main()
