import os
from typing import Optional
import requests
from requests.adapters import HTTPAdapter, Retry
from .logger import get_logger


REQUESTS_TIMEOUT = float(os.environ.get("REQUESTS_TIMEOUT", "10"))
REQUESTS_RETRIES = int(os.environ.get("REQUESTS_RETRIES", "3"))


class HttpClient:
    """HTTP client with retries and session pooling."""
    def __init__(self, timeout: float = REQUESTS_TIMEOUT, retries: int = REQUESTS_RETRIES):
        self.timeout = timeout
        self.session = requests.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        })
        self.logger = get_logger("HttpClient")

    def fetch(self, url: str) -> Optional[str]:
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            self.logger.warning("HTTP fetch failed for %s: %s", url, e)
            return None
