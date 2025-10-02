from typing import List
import threading
from queue import Queue
import sqlite3
from src.product import Product
from src.databases.awriter import AWriter


class SqliteWriter(AWriter):
    """
    Dedicated DB writer thread which consumes Product items from a queue and writes them to SQLite.
    Uses batched insert for efficiency.
    """

    def __init__(
        self, dsn: str,
        write_queue: Queue,
        batch_size: int = 20,
        stop_event: threading.Event = None
    ):
        super().__init__(write_queue, batch_size, stop_event, name="SQLiteWriter")
        self.dsn = dsn
        self._conn = None

    def run(self):
        # connect to database and run loop
        self._conn = sqlite3.connect(self.dsn, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._ensure_table()
        super().run()
        if self._conn:
            self._conn.close()

    def _write(self, items: List[Product]):
        rows = [p.as_tuple() for p in items]
        with self._conn:
            self._conn.executemany("""
                INSERT INTO vendr_products (name, description, category, min_price, max_price, median_price, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(name, category) DO UPDATE SET
                    description=excluded.description,
                    min_price=excluded.min_price,
                    max_price=excluded.max_price,
                    median_price=excluded.median_price,
                    scraped_at=CURRENT_TIMESTAMP
            """, rows)
        self.logger.info("Wrote %d products to DB.", len(rows))

    def _get_unique_key(self, item: Product):
        return (item.name, item.category)

    def _ensure_table(self):
        with self._conn:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS vendr_products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    category TEXT,
                    min_price INTEGER,
                    max_price INTEGER,
                    median_price INTEGER,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_product UNIQUE (name, category)
                );
            """)
        self.logger.debug("Ensured vendr_products table exists.")